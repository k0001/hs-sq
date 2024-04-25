module Sq.Transactional
   ( Transactional
   , embed
   , transactionalRetry
   , foldM
   , Ref
   , Retry (..)
   , retry
   , orElse
   ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM hiding (orElse, retry)
import Control.Exception.Safe qualified as Ex
import Control.Foldl qualified as F
import Control.Monad hiding (foldM)
import Control.Monad.Catch qualified as Cx
import Control.Monad.IO.Class
import Control.Monad.Ref hiding (Ref)
import Control.Monad.Ref qualified
import Control.Monad.Trans.Reader (ReaderT (ReaderT))
import Control.Monad.Trans.Resource qualified as R
import Control.Monad.Trans.Resource.Extra qualified as R hiding (runResourceT)
import Data.Acquire qualified as A
import Data.Coerce
import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap
import Data.Kind

import Sq.Connection
import Sq.Mode
import Sq.Statement
import Sq.Support

--------------------------------------------------------------------------------

-- | Used as the @r@ type-parameter in @'Transactional' g r t a@.
--
-- * If the 'Transactional' uses any 'Alternative' or 'MonadPlus' feature, then
-- @r@ must be 'Retry', and the 'Transactional' can only be executed through
-- 'Sq.read', 'Sq.commit' or 'Sq.rollback'.
--
-- * Otherwise, @r@ can be 'NoRetry'. In that case, 'embed' can
-- also be used to execute the 'Transactional'.
data Retry = NoRetry | Retry
   deriving (Eq, Ord, Show)

data Env (g :: k) (r :: Retry) (t :: Mode) = Env
   { unique :: STM Int
   -- ^ Next unique 'Int' within the 'Transactional' to be used as key in 'refs'
   , refs :: TVar (IntMap (SomeRef g))
   -- ^ Currently valid 'Ref's. We keep track of them in order to implement
   -- 'catch'. The 'IntMap' is just for fast diffing purposes.
   , tx :: Transaction t
   -- ^ Current transaction.
   }

acquireEnv :: Transaction t -> A.Acquire (Env g r t)
acquireEnv tx = do
   unique :: STM Int <- liftIO do
      tv <- newTVarIO 0
      pure $ atomicModifyRef' tv \i -> (i + 1, i)
   refs :: TVar (IntMap (SomeRef g)) <-
      R.mkAcquire1 (newTVarIO mempty) \tvsrs ->
         atomically do
            srs <- swapTVar tvsrs mempty
            forM_ srs \(SomeRef (Ref tv)) ->
               writeTVar tv Nothing
   pure Env{..}

-- | @'Transactional' g r t a@ groups together multiple interactions with a same
-- @'Transaction' t@ that finally produce a value of type @a@. Think of
-- 'Transactional' as if it was 'STM'.
--
-- * @g@ is an ephemeral tag for the whole inteaction group that prevents
-- 'Ref's and 'stream's from escaping its intended scope (like 'Data.STRef.ST'
-- does it). Just ignore it, it will always be polymorphic.
--
-- * @r@ says whether the 'Transactional' could potentially be retried from
-- scratch in order to observe a new snapshot of the database (like 'STM' does
-- it).  Learn more about this in 'Retry'.
--
-- * @t@ says whether the 'Transactional' could potentially perform 'Write'
-- or 'Read'-only operations.
--
-- * @a@ is the Haskell value finally produced by a successfu execution of
-- the 'Transactional'.
--
-- __To execute a 'Transactional'__ you will normally use one of 'Sq.read' or
-- 'Sq.commit' (or 'Sq.rollback' or 'Sq.embed', but those are less common).
--
-- @
-- /-- We are using 'Sq.commit' to execute the 'Transactional'. This means/
-- /-- that the transactional will have read and 'Write' capabilities, that/
-- /-- the transaction can 'Retry', and that ultimately, unless there are/
-- /-- unhandled exceptions, the changes will be commited to the database./
-- __"Sq".'Sq.commit' pool do__
--
--    /-- We can execute 'Write' 'Statement's:/
--    __userId1 <- "Sq".'Sq.one' /insertUser/ \"haskell\@example.com\"__
--
--    /-- And 'Read' 'Statement's:/
--    __userId2 \<- "Sq".'Sq.one' /getUserIdByEmail/ \"haskell\@example.com\"__
--
--    /-- We have 'MonadFail' too:/
--    __'when' (userId1 /= userId2) do__
--        __'fail' \"Something unexpected happened!\"__
--
--    /-- We also have 'Ref's, which work just like 'TVar's:/
--    __ref \<- 'newRef' (0 :: 'Int')__
--
--    /-- 'Ex.catch' behaves like 'catchSTM', undoing changes to 'Ref's/
--    /-- and to the database itself when the original action fails:/
--    __userId3 \<- 'Ex.catch'__
--        /-- Something will fail .../
--        __(do 'modifyRef' ref (+ 1)__
--            __\_ \<- "Sq".'Sq.one' /insertUser/ \"sqlite\@example.com\"__
--            __'Ex.throwM' FakeException123)__
--        /-- ... but there is a catch!/
--        __(\\FakeException123 -> do__
--            /-- The observable universe has been reset to what it/
--            /-- was before the 'Ex.catch':/
--            __"Sq".'Sq.zero' /getUserIdByEmail/ \"sqlite\@example.com\"__
--            __'modifyRef' ref (+ 10))__
--
--    /-- Only the effects from the exception handling function were preserved:/
--    __"Sq".'Sq.zero' /getUserIdByEmail/ \"sqlite\@example.com\"__
--    __10 <- 'readRef' ref__
--
--    /-- 'retry' and its synonyms 'mzero' and 'empty' not only discard changes as/
--    /-- 'Ex.catch' does, but they also cause the ongoing 'Transaction' to be/
--    /-- discarded, and the /-- entire 'Transactional' to be executed again on a/
--    /-- brand new 'Transaction' observing a new snapshot of the database. For/
--    /-- example, the following code will keep retrying the whole 'Transactional'/
--    /--  until the user with the specified email exists./
--    __userId4 \<- "Sq".'maybe' /getUserIdByEmail/ \"nix@example.com\" >>= \\case__
--        __'Just' x -> 'pure' x__
--        __'Nothing' -> 'retry'__
--
--    /-- Presumably, this example was waiting for a concurrent connection to/
--    /-- insert said user. If we got here, it is because that happened./
--
--    /-- As usual, 'mzero' and 'empty' can be handled by means of '<|>' and 'mplus',/
--    /-- or its synonym 'orElse'./
--    __'False' \<- 'mplus' ('writeRef' ref 8 >> 'mzero' >> 'pure' 'True')__
--                   __('pure' 'False')__
--
--    /-- The recent 'writeRef' to 8 on the 'retry'ied 'Transactional' was discarded:/
--    __10 <- 'readRef' ref__
--
--    __'pure' ()__
-- @
newtype Transactional (g :: k) (r :: Retry) (t :: Mode) (a :: Type)
   = Transactional (Env g r t -> R.ResourceT IO a)
   deriving
      ( Functor
      , Applicative
      , Monad
      , Cx.MonadThrow
      , Cx.MonadMask
      , MonadFail
      )
      via (ReaderT (Env g r t) (R.ResourceT IO))

-- | INTERNAL only. This doesn't deal with @g@.
un :: Transactional g r t a -> Env g r t -> R.ResourceT IO a
un = coerce
{-# INLINE un #-}

mk :: (Env g r t -> R.ResourceT IO a) -> Transactional g r t a
mk = coerce
{-# INLINE mk #-}

-- | INTERNAL. Used to implement 'Sq.read', 'Sq.commit' and 'Sq.rollback'.
--
-- Run all the actions in a 'Transactional' as part of a single 'Transaction'.
transactionalRetry
   :: forall m r t a
    . (MonadIO m)
   => A.Acquire (Transaction t)
   -> (forall g. Transactional g r t a)
   -> m a
transactionalRetry atx ta = liftIO (go 0)
  where
   go :: Word -> IO a
   go !n = Ex.catch once \ErrRetry -> do
      -- TODO: Wait with `sqlite3_commit_hook` instead of just retrying.
      let ms = logBase 2 (fromIntegral (max 1 n) :: Double)
      threadDelay $ truncate (1_000 * ms)
      go (n + 1)
   once :: IO a
   once = R.runResourceT do
      (_, env) <- A.allocateAcquire $ acquireEnv =<< atx
      un ta env

-- | Embeds all the actions in a 'Transactional' as part of an ongoing
-- 'Transaction'.
--
-- * __NOTICE__ Contrary to 'Sq.read', 'Sq.commit' or 'Sq.rollback',
-- this 'Transactional' cannot 'retry', as doing so would require
-- cancelling the ongoing 'Transaction'.
embed
   :: forall m t a
    . (MonadIO m)
   => Transaction t
   -- ^ Ongoing transaction.
   -> (forall g. Transactional g 'NoRetry t a)
   -> m a
embed tx ta =
   liftIO $ R.runResourceT do
      (_, env) <- A.allocateAcquire $ acquireEnv tx
      un ta env

-- | __Impurely fold__ the output rows.
--
-- * For a non-'Transactional' version of this function, see 'Sq.foldIO'.
foldM
   :: (SubMode t s)
   => F.FoldM (Transactional g r t) o z
   -> Statement s i o
   -> i
   -> Transactional g r t z
foldM f st i = mk \env ->
   foldIO (F.hoists (flip un env) f) (pure env.tx) st i

-- | 'Ex.catch' behaves like "STM"'s 'catchSTM'.
--
-- In @'Ex.catch' ma f@, if an exception is thrown by @ma@, then any
-- database or 'Ref' changes made by @ma@ will be discarded. Furthermore, if
-- @f@ can handle said exception, then the action resulting from applying @f@
-- will be executed. Otherwise, if @f@ can't handle the exception, it will
-- bubble up.
--
-- Note: This instance's 'Cx.catch' catches async exceptions because that's
-- what 'Cx.MonadCatch' instances normaly do. As a user of this instance, you
-- probably want to use "Control.Exceptions.Safe" to make sure you don't catch
-- async exceptions unless you really want to.
instance Ex.MonadCatch (Transactional g r t) where
   catch act f = mk \env -> do
      refsRollback <- liftIO $ atomically $ saveSomeRefs env.refs
      case env.tx.smode of
         SRead ->
            Ex.catchAsync (un act env) \se -> do
               liftIO $ atomically refsRollback
               case Ex.fromException se of
                  Nothing -> Ex.throwM se
                  Just e -> un (f e) env
         SWrite -> Ex.mask \restore -> do
            sp <- savepoint env.tx
            Ex.tryAsync (restore (un act env)) >>= \case
               Right a -> do
                  -- savepointRelease is not critical.
                  void $ Ex.tryAny $ savepointRelease sp
                  pure a
               Left se -> do
                  liftIO $ atomically refsRollback
                  savepointRollback sp
                  -- savepointRelease is not critical. Just making sure we
                  -- don't accumulate many savepoints in case there is some
                  -- recursion going on.
                  void $ Ex.tryAny $ savepointRelease sp
                  case Ex.fromException se of
                     Nothing -> Ex.throwM se
                     Just e -> restore $ un (f e) env

--------------------------------------------------------------------------------

-- | INTERNAL.
data ErrRetry = ErrRetry
   deriving stock (Show)
   deriving anyclass (Ex.Exception)

-- | 'retry' behaves like 'STM'\'s 'Control.Concurrent.STM.retry'. It causes
-- the current 'Transaction' to be cancelled so that a new one can take its
-- place and the entire 'Transactional' is executed again. This allows the
-- 'Transactional' to observe a new snapshot of the database.
--
-- * 'retry', 'empty' and 'mzero' all do fundamentally the same thing,
-- however 'retry' leads to better type inferrence because it forces the
-- @r@ type-parameter to be 'Retry'.
--
-- * __NOTICE__ You only need to use 'mzero' if you need access to a newer
-- database snapshot. If all you want to do is undo some 'Ref' transformation
-- effects, or undo database changes, then use 'catch' which doesn't abandon
-- the 'Transaction'.
--
-- * __WARNING__ If we keep 'retry'ing and the database never changes, then
-- we will be stuck in a loop forever. To mitigate this, when executing the
-- 'Transactional' through 'Sq.read', 'Sq.commit' or 'Sq.rollback', you may
-- want to use 'System.Timeout.timeout' to abort at some point in the future.
retry :: Transactional g 'Retry t a
retry = Ex.throwM ErrRetry
{-# INLINE retry #-}

-- | @'orElse' ma mb@ behaves like 'STM'\'s @'Control.Concurrent.STM.orElse' ma
-- mb@.  If @ma@ completes without executing 'retry', then that constitutes the
-- entirety of @'orElse' ma mb@. Otherwise, if @ma@ executed 'retry', then all
-- the effects from @ma@ are discared and @mb@ is tried in its place.
--
-- * 'orElse', '<|>' and 'mplus' all do the same thing, but 'orElse' has a more
-- general type because it doesn't force the @r@ type-parameter to be 'Retry'.
orElse
   :: Transactional g r t a
   -> Transactional g r t a
   -> Transactional g r t a
orElse tl tr = Ex.catch tl \ErrRetry -> tr

-- | @
-- 'empty' = 'retry'
-- '(<|>)' = 'orElse'
-- @
instance Alternative (Transactional g 'Retry t) where
   empty = retry
   {-# INLINE empty #-}
   (<|>) = orElse
   {-# INLINE (<|>) #-}

-- | @
-- 'mzero' = 'retry'
-- 'mplus' = 'orElse'
-- @
instance MonadPlus (Transactional g 'Retry t) where
   mzero = retry
   {-# INLINE mzero #-}
   mplus = orElse
   {-# INLINE mplus #-}

--------------------------------------------------------------------------------

data SomeRef g where
   SomeRef :: Ref g a -> SomeRef g

-- | Creates a “savepoint” with the current state of the given 'SomeRef's.
-- The produced 'STM' action can be used to rollback the 'SomeRef's current
-- state in the future.
saveSomeRefs :: TVar (IntMap (SomeRef g)) -> STM (STM ())
saveSomeRefs tvsrs = do
   srs0 <- readTVar tvsrs
   rollbacks <- forM srs0 \(SomeRef (Ref tv)) ->
      writeTVar tv <$> readTVar tv
   pure do
      srs1 <- swapTVar tvsrs srs0
      forM_ (IntMap.difference srs1 srs0) \(SomeRef (Ref tv)) ->
         writeTVar tv Nothing
      sequence_ rollbacks

-- | Like 'TVar', but you can use it inside 'Transactional' through the
-- 'MonadRef' and 'MonadAtomicRef' vocabulary.
newtype Ref g a = Ref (TVar (Maybe a))
   deriving newtype
      ( Eq
        -- ^ Pointer equality
      )

-- | All operations are atomic.
instance MonadRef (Transactional g r t) where
   type Ref (Transactional g r t) = Sq.Transactional.Ref g
   newRef a = mk \env -> liftIO $ atomically do
      i <- env.unique
      tv <- newTVar $! Just a
      let ref = Ref tv
      -- Note: We only explicitly remove things from the IntMap through
      -- saveSomeRefs, or when exiting Transactional. Maybe some day we
      -- optimize this.
      modifyTVar' env.refs $ IntMap.insert i $! SomeRef ref
      pure ref
   readRef (Ref tv) = mk \_ -> liftIO $ atomically do
      readTVar tv >>= \case
         Just a -> pure a
         Nothing -> Ex.throwM $ resourceVanishedWithCallStack "Ref"
   writeRef r a = atomicModifyRef r \_ -> (a, ())
   modifyRef r f = atomicModifyRef r \a -> (f a, ())
   modifyRef' r f = atomicModifyRef' r \a -> (f a, ())

instance MonadAtomicRef (Transactional g r t) where
   atomicModifyRef (Ref tv) f =
      mk \_ -> liftIO $ atomically do
         readTVar tv >>= \case
            Just a0 | (a1, b) <- f a0 -> do
               writeTVar tv $! Just a1
               pure b
            Nothing -> Ex.throwM $ resourceVanishedWithCallStack "Ref"
   atomicModifyRef' (Ref tv) f =
      mk \_ -> liftIO $ atomically do
         readTVar tv >>= \case
            Just a0 | (!a1, !b) <- f a0 -> do
               writeTVar tv $! Just a1
               pure b
            Nothing -> Ex.throwM $ resourceVanishedWithCallStack "Ref"
