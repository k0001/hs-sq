module Sq.Transactional
   ( Transactional
   , transactional
   , foldM
   , stream
   , Ref
   ) where

import Control.Concurrent.STM
import Control.Exception.Safe qualified as Ex
import Control.Foldl qualified as F
import Control.Monad hiding (foldM)
import Control.Monad.Catch qualified as Cx
import Control.Monad.IO.Class
import Control.Monad.Ref hiding (Ref)
import Control.Monad.Ref qualified
import Control.Monad.Trans.Class
import Control.Monad.Trans.Reader
import Control.Monad.Trans.Resource qualified as R
import Data.Acquire qualified as A
import Data.Coerce
import Data.IORef
import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap
import Data.Kind
import Streaming qualified as Z

import Sq.Connection
import Sq.Mode
import Sq.Statement
import Sq.Support

--------------------------------------------------------------------------------

-- | @'Transactional' g t a@ groups together multiple interactions with a same
-- @'Transaction' t@ that finally produce a value of type @a@.
--
-- @g@ is a unique tag for the whole inteaction group that prevents actions
-- from escaping its intended scope (like 'Data.STRef.ST' does it).
-- Particularly, we don't want 'Ref's nor 'stream's to escape.
--
-- Run an 'Transactional' with 'transactional'. Example:
--
-- @
-- "Sq".'transactional' pool.write do
--    x <- "Sq".'one' myStatement 123
--    "Sq".'list' anotherStatement x
-- @
newtype Transactional (g :: k) (t :: Mode) (a :: Type)
   = Transactional
      ( (STM Int, TVar (IntMap (SomeRef g)), Transaction t)
        -> R.ResourceT IO a
      )
   deriving
      ( Functor
      , Applicative
      , Monad
      , Ex.MonadThrow
      , Ex.MonadMask
      )
      via ( ReaderT
               (STM Int, TVar (IntMap (SomeRef g)), Transaction t)
               (R.ResourceT IO)
          )

-- | Internal only. This doesn't deal with @g@.
unTransactional
   :: Transactional g t a
   -> (STM Int, TVar (IntMap (SomeRef g)), Transaction t)
   -> R.ResourceT IO a
unTransactional = coerce
{-# INLINE unTransactional #-}

-- | Run an 'Transactional' with 'transactional'. Example:
--
-- @
-- "Sq".'transactional' pool.write do
--    x <- "Sq".'onei' myStatement 123
--    "Sq".'list' anotherStatement x
-- @
transactional
   :: (MonadIO m)
   => A.Acquire (Transaction t)
   -- ^ How to obtain the 'Transaction' once @m@ is evaluated.
   --
   -- Most likely you will be using one of @pool.read@ or @pool.commit@ here.
   -- That is, 'Sq.read' or 'Sq.commit'. This corresponds to the idea that
   -- no 'Statement' other than the one given to this function will be
   -- executed during the 'Transaction'.
   --
   -- If you already obtained a 'Transaction' by other means, then simply use
   -- 'pure' to wrap a 'Transaction' in 'A.Acquire'.
   -> (forall g. Transactional g t a)
   -> m a
transactional atx (Transactional f) = liftIO $ R.runResourceT do
   sint :: STM Int <- liftIO do
      tv <- newTVarIO 0
      pure $ atomicModifyRef' tv \i -> (i + 1, i)
   (_, tvsrs :: TVar (IntMap (SomeRef g))) <-
      R.allocate (newTVarIO mempty) \tvsrs ->
         atomically do
            srs <- swapTVar tvsrs mempty
            forM_ srs \(SomeRef (Ref tv)) ->
               writeTVar tv Nothing
   (_, tx :: Transaction t) <- A.allocateAcquire atx
   f (sint, tvsrs, tx)

-- | Like 'Sq.foldIO', but runs in 'Transactional'.
foldM
   :: (SubMode t s)
   => F.FoldM (Transactional g t) o z
   -> Statement s i o
   -> i
   -> Transactional g t z
foldM f st i = Transactional \env@(_, _, tx) ->
   foldIO (F.hoists (flip unTransactional env) f) (pure tx) st i

-- | Like 'Sq.streamIO', but runs in 'Transactional'.
stream
   :: (SubMode t s)
   => Statement s i o
   -> i
   -> Z.Stream (Z.Of o) (Transactional g t) ()
stream = \st i -> do
   ioref <- lift $ Transactional \(_, _, tx) -> liftIO $ newIORef tx
   Z.hoist (Transactional . const) $ streamIO (liftIO (readIORef ioref)) st i

-- | 'Ex.catch' behaves like 'Control.Concurrent.STM.catchSTM'.
--
-- @'Ex.catch' foo bar@ catches any exception thrown by @foo@ using the
-- function @bar@ to handle the exception. If an exception is thrown by @foo@,
-- any 'Transaction'al and 'Ref' changes made by @foo@ are rolled back,
-- but changes prior to @foo@ persist.
instance Ex.MonadCatch (Transactional g t) where
   catch act f = Transactional \env@(_, tvsrs, tx) -> do
      srsRollback <- liftIO $ atomically $ saveSomeRefs tvsrs
      case tx.smode of
         SRead -> Ex.catch (unTransactional act env) \e -> do
            liftIO $ atomically srsRollback
            unTransactional (f e) env
         SWrite ->
            Ex.bracketWithError
               (savepoint tx)
               ( \ye sp -> case ye of
                  Nothing ->
                     void $ Ex.tryAny $ savepointRelease sp -- not critical
                  Just se -> do
                     savepointRollback sp
                     liftIO $ atomically srsRollback
                     void $ Ex.tryAny $ savepointRelease sp -- not critical
                     forM_ (Ex.fromException se) \e ->
                        unTransactional (f e) env
               )
               (\_ -> unTransactional act env)

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
instance MonadRef (Transactional g t) where
   type Ref (Transactional g t) = Sq.Transactional.Ref g
   newRef a = Transactional \(sint, tvs, _) -> liftIO $ atomically do
      i <- sint
      tv <- newTVar $! Just a
      let ref = Ref tv
      -- Note: We only explicitly remove things from the IntMap through
      -- saveSomeRefs, or when exiting Transactional. Maybe some day we optimize this.
      modifyTVar' tvs $ IntMap.insert i $! SomeRef ref
      pure ref
   readRef (Ref tv) = Transactional \_ -> liftIO $ atomically do
      readTVar tv >>= \case
         Just a -> pure a
         Nothing -> Ex.throwM $ resourceVanishedWithCallStack "Ref"
   writeRef r a = atomicModifyRef r \_ -> (a, ())
   modifyRef r f = atomicModifyRef r \a -> (f a, ())
   modifyRef' r f = atomicModifyRef' r \a -> (f a, ())

instance MonadAtomicRef (Transactional g t) where
   atomicModifyRef (Ref tv) f = Transactional \_ -> liftIO $ atomically do
      readTVar tv >>= \case
         Just a0 | (a1, b) <- f a0 -> do
            writeTVar tv $! Just a1
            pure b
         Nothing -> Ex.throwM $ resourceVanishedWithCallStack "Ref"
   atomicModifyRef' (Ref tv) f = Transactional \_ -> liftIO $ atomically do
      readTVar tv >>= \case
         Just a0 | (!a1, !b) <- f a0 -> do
            writeTVar tv $! Just a1
            pure b
         Nothing -> Ex.throwM $ resourceVanishedWithCallStack "Ref"
