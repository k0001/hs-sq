{-# LANGUAGE StrictData #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoFieldSelectors #-}

module Sqlime.Internal where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception qualified as BEx
import Control.Exception.Safe qualified as Ex
import Control.Monad
import Control.Monad.Codensity
import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.Reader
import Control.Monad.Trans.Resource qualified as R hiding (runResourceT)
import Control.Monad.Trans.Resource.Extra qualified as R
import Control.Retry qualified as Retry
import Data.Acquire qualified as A
import Data.Bool
import Data.Char qualified as Ch
import Data.Coerce
import Data.Foldable
import Data.Functor.Contravariant
import Data.Functor.Contravariant.Divisible
import Data.Int
import Data.List.NonEmpty qualified as NEL
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Monoid
import Data.Profunctor
import Data.String
import Data.Text qualified as T
import Database.SQLite3 qualified as S
import GHC.IO.Exception
import GHC.Show
import GHC.Stack
import Language.Haskell.TH.Quote (QuasiQuoter (..))
import Streaming qualified as Z
import Streaming.Prelude qualified as Z

--------------------------------------------------------------------------------

-- | The @NULL@ SQL datatype.
data Null = Null
   deriving stock (Eq, Ord, Show)

instance Semigroup Null where
   _ <> _ = Null

instance Monoid Null where
   mempty = Null

--------------------------------------------------------------------------------

-- | Ideally, encoding a value of type @a@ should never fail. However, that
-- implies that @a@ should be small enough that all of its inhabitants can be
-- safely represented as 'S.SQLData', which in practice often means that @a@
-- will often have to be a @newtype@ leading to a lot of boilerplate. To help
-- reduce that boilerplate, we allow the 'Encoder' to fail.
--
-- See 'encodeSizedIntegral' as a motivating example.
newtype Encoder a = Encoder (a -> Either Ex.SomeException S.SQLData)
   deriving
      (Contravariant)
      via Op (Either Ex.SomeException S.SQLData)

runEncoder :: Encoder a -> a -> Either Ex.SomeException S.SQLData
runEncoder = coerce

--------------------------------------------------------------------------------

data ErrDecoder
   = -- | Got, expected.
     ErrDecoder_Type S.ColumnType [S.ColumnType]
   | ErrDecoder_Fail Ex.SomeException
   deriving stock (Show)
   deriving anyclass (Ex.Exception)

newtype Decoder a
   = Decoder (S.SQLData -> Either ErrDecoder a)
   deriving
      (Functor, Applicative, Monad)
      via ReaderT S.SQLData (Either ErrDecoder)

runDecoder
   :: Decoder a -> S.SQLData -> Either ErrDecoder a
runDecoder = coerce

-- | @'mempty' = 'pure' 'mempty'@
instance (Monoid a) => Monoid (Decoder a) where
   mempty = pure mempty

-- | @('<>') == 'liftA2' ('<>')@
instance (Semigroup a) => Semigroup (Decoder a) where
   (<>) = liftA2 (<>)

instance Ex.MonadThrow Decoder where
   throwM = Decoder . const . Left . ErrDecoder_Fail . Ex.toException

instance MonadFail Decoder where
   fail = Ex.throwString

-- | Leftmost result on success, rightmost error on failure.
instance Alternative Decoder where
   empty = fail "empty"
   (<|>) = mplus

-- | Leftmost result on success, rightmost error on failure.
instance MonadPlus Decoder where
   mzero = fail "mzero"
   mplus = \l r -> Decoder \s ->
      either (\_ -> runDecoder r s) pure (runDecoder l s)

--------------------------------------------------------------------------------

newtype Name = Name T.Text
   deriving newtype (Eq, Ord, Show)

instance IsString Name where
   fromString = either error id . name . T.pack

unName :: Name -> T.Text
unName = coerce

-- | ASCII letters, digits or underscores.
-- First character must be letter or underscore.
name :: T.Text -> Either String Name
name t0 = do
   (c0, t1) <- maybe err1 pure $ T.uncons t0
   unless
      (c0 == '_' || (Ch.isAscii c0 && Ch.isAlpha c0))
      err1
   unless
      ( T.all
         (\c -> c == '_' || (Ch.isAscii c && (Ch.isAlpha c || Ch.isDigit c)))
         t1
      )
      err2
   pure $ Name t0
  where
   err1 = Left "Expected ASCII letter or digit"
   err2 = Left "Expected ASCII letter, digit or underscore"

--------------------------------------------------------------------------------

data BindingName = BindingName Name [Name]
   deriving stock (Eq, Ord)

instance Show BindingName where
   showsPrec n = showsPrec n . renderBindingName

bindingName :: Name -> BindingName
bindingName n = BindingName n []

consBindingName :: Name -> BindingName -> BindingName
consBindingName a (BindingName b cs) = BindingName a (b : cs)

-- | @$foo::bar::baz@
renderBindingName :: BindingName -> T.Text
renderBindingName (BindingName x xs) =
   T.cons '$' $ T.intercalate "::" $ fmap unName (x : xs)

--------------------------------------------------------------------------------

newtype Input i
   = Input (i -> Map.Map BindingName (Either Ex.SomeException S.SQLData))
   deriving newtype
      ( Semigroup
        -- ^ Left-biased.
      , Monoid
      )
   deriving
      (Contravariant, Divisible, Decidable)
      via Op (Map.Map BindingName (Either Ex.SomeException S.SQLData))

runInput
   :: Input i -> i -> Map.Map BindingName (Either Ex.SomeException S.SQLData)
runInput = coerce

data ErrBinding = ErrBinding BindingName Ex.SomeException
   deriving stock (Show)
   deriving anyclass (Ex.Exception)

encode :: Name -> Encoder i -> Input i
encode n e = Input (Map.singleton (bindingName n) . runEncoder e)

push :: Name -> Input i -> Input i
push n ba = Input \s ->
   Map.mapKeysMonotonic (consBindingName n) (runInput ba s)

bindStatement :: S.Statement -> Input i -> i -> IO ()
bindStatement st ii i = do
   !m <-
      Map.mapKeysMonotonic renderBindingName
         <$> Map.traverseWithKey
            (\bn -> either (Ex.throwM . ErrBinding bn) pure)
            (runInput ii i)
   S.bindNamed st $! Map.toAscList m

--------------------------------------------------------------------------------

data Output o
   = Output_Pure o
   | Output_Fail Ex.SomeException
   | Output_Decode Name (Decoder (Output o))

data ErrOutput
   = ErrOutput_ColumnValue Name ErrDecoder
   | ErrOutput_ColumnMissing Name
   | ErrOutput_Fail Ex.SomeException
   deriving stock (Show)
   deriving anyclass (Ex.Exception)

decode :: Name -> Decoder o -> Output o
decode n vda = Output_Decode n (Output_Pure <$> vda)

runOutput
   :: (Monad m)
   => (Name -> m (Maybe S.SQLData))
   -> Output o
   -> m (Either ErrOutput o)
runOutput f = \case
   Output_Decode n vda -> do
      f n >>= \case
         Just s -> case runDecoder vda s of
            Right d -> runOutput f d
            Left e -> pure $ Left $ ErrOutput_ColumnValue n e
         Nothing -> pure $ Left $ ErrOutput_ColumnMissing n
   Output_Pure a -> pure $ Right a
   Output_Fail e -> pure $ Left $ ErrOutput_Fail e

instance Functor Output where
   fmap = liftA

instance Applicative Output where
   pure = Output_Pure
   liftA2 = liftM2

instance Monad Output where
   Output_Decode n vda >>= k =
      Output_Decode n (fmap (>>= k) vda)
   Output_Pure a >>= k = k a
   Output_Fail e >>= _ = Output_Fail e

instance Ex.MonadThrow Output where
   throwM = Output_Fail . Ex.toException

instance MonadFail Output where
   fail = Ex.throwString

instance (Semigroup o) => Semigroup (Output o) where
   (<>) = liftA2 (<>)

instance (Monoid o) => Monoid (Output o) where
   mempty = pure mempty

--------------------------------------------------------------------------------

newtype ConnectionString = ConnectionString T.Text
   deriving newtype (Eq, Ord, Show, IsString)

--------------------------------------------------------------------------------

newtype RawStatement = RawStatement T.Text
   deriving newtype (Eq, Ord, Show, IsString)

rawStatement :: QuasiQuoter
rawStatement =
   QuasiQuoter
      { quoteExp = \s -> [|fromString @RawStatement s|]
      , quotePat = \_ -> fail "rawStatement: No quotePat"
      , quoteType = \_ -> fail "rawStatement: No quoteType"
      , quoteDec = \_ -> fail "rawStatement: No quoteDec"
      }

--------------------------------------------------------------------------------

data ErrConnection = ErrConnection_Deadlock
   deriving stock (Eq, Show)
   deriving anyclass (Ex.Exception)

-- | A database connection handle. It's safe to try to use this 'Connection'
-- concurrently.
data Connection = Connection
   { lock :: MVar (Maybe (Codensity IO S.Database))
   -- ^ Lock is so that Connection users don't step on each other.
   -- For example, `Transaction` will hold this lock while active.
   -- 'Nothing' if is not possible to access the database from here anymore.
   , statements :: MVar (Map RawStatement PreparedStatement)
   }

acquireConnectionLock
   :: String -> Connection -> A.Acquire (Codensity IO S.Database)
acquireConnectionLock desc conn = do
   R.mkAcquire1
      ( Ex.catchAsync
         (takeMVar conn.lock)
         (\BEx.BlockedIndefinitelyOnMVar -> Ex.throwM ErrConnection_Deadlock)
         >>= \case
            Just x -> pure x
            Nothing -> do
               putMVar conn.lock Nothing
               Ex.throwM $ resourceVanishedWithCallStack desc
      )
      ( -- "try" because the 'Connection' release might have filled
        -- `conn.lock` with Nothing
        void . tryPutMVar conn.lock . Just
      )

data DatabaseMessage
   = forall x.
      DatabaseMessage
      (S.Database -> IO x)
      (Either Ex.SomeException x -> IO ())

acquireConnection
   :: ConnectionString
   -> [S.SQLOpenFlag]
   -> S.SQLVFS
   -> A.Acquire Connection
acquireConnection (ConnectionString t) flags vfs = do
   statements :: MVar (Map RawStatement PreparedStatement) <-
      R.mkAcquire1 (newMVar mempty) \mv ->
         takeMVar mv >>= traverse_ \ps ->
            Ex.catchAsync @_ @Ex.SomeException (S.finalize ps.handle) mempty
   -- We run all interactions with the database through a single background
   -- thread because it makes it easy to deal with async exceptions. We acquire
   -- the database handle in that thread to prevent potential problems if
   -- SQLite expects a particular OS thread.
   mvDM :: MVar DatabaseMessage <- liftIO newEmptyMVar
   mvOpen :: MVar (Maybe Ex.SomeException) <- liftIO newEmptyMVar
   mvStart :: MVar () <- liftIO newEmptyMVar
   _ <- flip R.mkAcquire1 killThread do
      self <- myThreadId
      forkFinally
         (background (putMVar mvOpen) (takeMVar mvStart) (takeMVar mvDM))
         \case
            Left se
               | Just BEx.ThreadKilled <- Ex.fromException se -> pure ()
               | otherwise -> Ex.throwTo self se
            _ -> pure ()
   -- If opening the database failed, rethrow the exception synchronously.
   liftIO $ takeMVar mvOpen >>= maybe (pure ()) Ex.throwM
   -- Only now we allow the background thread to start working, so that
   -- we don't receive its async exceptions too early.
   liftIO $ putMVar mvStart ()
   lock :: MVar (Maybe (Codensity IO S.Database)) <-
      R.mkAcquire1
         ( newMVar $ Just $ Codensity \act -> do
            mv <- newEmptyMVar
            putMVar mvDM $! DatabaseMessage act $ putMVar mv
            takeMVar mv >>= either Ex.throwM pure
         )
         (\mv -> takeMVar mv >> putMVar mv Nothing)
   pure Connection{statements, lock}
  where
   background
      :: forall x
       . (Maybe Ex.SomeException -> IO ())
      -> IO ()
      -> IO DatabaseMessage
      -> IO x
   background onOpen waitStart next =
      Ex.bracket
         ( Ex.withException (S.open2 t flags vfs) (onOpen . Just)
            <* onOpen Nothing
         )
         ( \h ->
            Ex.uninterruptibleMask_ (S.interrupt h)
               `Ex.finally` S.close h
         )
         \h -> do
            waitStart
            forever do
               DatabaseMessage act res <- next
               Ex.try (act h) >>= res

--------------------------------------------------------------------------------

-- | A database transaction handle. It's safe to try to use this 'Connection'
-- concurrently.
--
-- While the 'Transaction' is active, an exclusive lock is held on the
-- underlying 'Connection'.
newtype Transaction
   = -- | This 'Connection' is a wrapper around the exclusively-locked
     -- underlying 'Connection', which can be used to interact safely with it.
     Transaction Connection

-- | @BEGIN@s a database transaction. If released with 'A.ReleaseExceptionWith',
-- then the transaction is @ROLLBACK@ed. Otherwise, it is @COMMIT@ed.
acquireTransaction :: Connection -> A.Acquire Transaction
acquireTransaction conn = do
   -- While the Transaction is active, it will hold an exclusive lock on `conn`.
   Codensity run' <- acquireConnectionLock "Connection" conn
   R.mkAcquireType1 (run' (flip S.exec "BEGIN")) $ const \case
      A.ReleaseExceptionWith _ -> run' (flip S.exec "ROLLBACK")
      _ ->
         -- We keep retrying to commit if the database is busy.
         Retry.recovering
            (Retry.constantDelay 50_000) -- 50 ms
            [\_ -> Ex.Handler \e -> pure (S.sqlError e == S.ErrorBusy)]
            (\_ -> run' (flip S.exec "COMMIT"))
   lock :: MVar (Maybe (Codensity IO S.Database)) <-
      R.mkAcquire1
         (newMVar $ Just $ Codensity run')
         (\mv -> takeMVar mv >> putMVar mv Nothing)
   pure $ Transaction Connection{statements = conn.statements, lock}

--------------------------------------------------------------------------------

-- | A statements statement taking a value @i@ as input and producing rows of
-- @o@ values as output.
data Statement i o = Statement
   { unsafeFFI :: Bool
   , input :: Input i
   , output :: Output o
   , raw :: RawStatement
   }

instance Show (Statement i o) where
   showsPrec n s =
      showParen (n >= appPrec1) $
         showString "Statement{raw = "
            . shows s.raw
            . showString ", unsafeFFI = "
            . shows s.unsafeFFI
            . showString "}"

statement :: Input i -> Output o -> RawStatement -> Statement i o
statement input output raw = Statement{unsafeFFI = False, ..}

instance Functor (Statement i) where
   fmap = rmap

instance Profunctor Statement where
   dimap f g s = s{input = f >$< s.input, output = g <$> s.output}

--------------------------------------------------------------------------------

data PreparedStatement = PreparedStatement
   { handle :: S.Statement
   , unsafeFFI :: Bool
   }

acquirePreparedStatement
   :: Statement i o
   -> Connection
   -> A.Acquire PreparedStatement
acquirePreparedStatement st conn = do
   R.mkAcquire1
      ( modifyMVar conn.statements \m0 ->
         case mapPop st.raw m0 of
            (Just ps, m1) -> pure (m1, ps)
            (Nothing, m1) ->
               A.with
                  (acquireConnectionLock "Connection" conn)
                  \(Codensity run) ->
                     run \dbh -> do
                        sth <- S.prepare dbh $ case st.raw of
                           RawStatement t -> t
                        pure (m1, PreparedStatement sth st.unsafeFFI)
      )
      \ps -> flip Ex.onException (S.finalize ps.handle) do
         S.reset ps.handle
         modifyMVar_ conn.statements (pure . Map.insert st.raw ps)

--------------------------------------------------------------------------------

getStatementColumnNameIndexes :: S.Statement -> IO (Map Name S.ColumnIndex)
getStatementColumnNameIndexes st = do
   -- Despite the type name, ncols is a length.
   S.ColumnIndex (ncols :: Int) <- S.columnCount st
   foldM
      ( \ !m i -> do
         -- Pattern never fails because `i` is in range.
         Just t <- S.columnName st i
         case name t of
            Right n ->
               Map.alterF
                  ( \case
                     Nothing -> pure $ Just i
                     Just _ -> Ex.throwM $ ErrStatement_DuplicateColumnName n
                  )
                  n
                  m
            Left _ ->
               -- If `t` is not a valid `Name`, we can just ignore it.
               -- It just won't be available for lookup by the RowEncoder.
               pure m
      )
      Map.empty
      (S.ColumnIndex <$> enumFromTo 0 (ncols - 1))

data ErrStatement
   = ErrStatement_DuplicateColumnName Name
   deriving stock (Eq, Show)
   deriving anyclass (Ex.Exception)

--------------------------------------------------------------------------------

data ErrRows
   = ErrRows_TooFew
   | ErrRows_TooMany
   deriving stock (Eq, Show)
   deriving anyclass (Ex.Exception)

-- | Like 'rowsList'. Throws 'ErrRows_TooFew' if no rows.
row :: (MonadIO m) => Statement i o -> i -> Transaction -> m o
row st i tx = liftIO do
   rowMaybe st i tx >>= \case
      Just o -> pure o
      Nothing -> Ex.throwM ErrRows_TooFew

-- | Like 'rowsList'. Throws 'ErrRows_TooMany' if more than 1 row.
rowMaybe :: (MonadIO m) => Statement i o -> i -> Transaction -> m (Maybe o)
rowMaybe st i tx = liftIO $ R.runResourceT do
   Z.next (rowsStream st i tx) >>= \case
      Right (o, z1) ->
         Z.next z1 >>= \case
            Left () -> pure (Just o)
            Right _ -> Ex.throwM ErrRows_TooMany
      Left () -> pure Nothing

-- | Like 'rowsList'. Throws 'ErrRows_TooFew' if no rows.
rowsNonEmpty
   :: (MonadIO m)
   => Statement i o
   -> i
   -> Transaction
   -> m (Int64, NEL.NonEmpty o)
rowsNonEmpty st i tx = liftIO do
   rowsList st i tx >>= \case
      (n, os) | Just nos <- NEL.nonEmpty os -> pure (n, nos)
      _ -> Ex.throwM ErrRows_TooFew

-- | Get the statement output rows as a list, together with its length.
--
-- Holds an exclusive lock on the database connection temporarily, while the
-- list is being constructed.
rowsList :: (MonadIO m) => Statement i o -> i -> Transaction -> m (Int64, [o])
rowsList st i tx =
   liftIO
      $ R.runResourceT
      $ Z.fold_
         (\(!n, !e) o -> (n + 1, e <> Endo (o :)))
         (0, mempty)
         (fmap (flip appEndo []))
      $ rowsStream st i tx

-- | Stream of output rows.
--
-- __An exclusive lock__ will be held on the database while this 'Z.Stream' is
-- being produced.
--
-- __You must exit @m@__, with 'R.runResourceT' or similar, for the transaction
-- lock and related resources to be released.
--
-- __As a convenience__, if the 'Z.Stream' is /fully/ consumed, the resources
-- associated with it will be released right away, meaning you can defer
-- exiting @m@ until later.
rowsStream
   :: (R.MonadResource m)
   => Statement i o
   -> i
   -> Transaction
   -> Z.Stream (Z.Of o) m ()
rowsStream st i (Transaction conn) = do
   (k0, ps) <- lift $ A.allocateAcquire $ acquirePreparedStatement st conn
   k1 <- lift do
      R.allocate_
         (bindStatement ps.handle st.input i)
         (S.clearBindings ps.handle)
   (k2, _) <- lift do
      A.allocateAcquire $ acquireConnectionLock "Transaction" conn
   ixs <- liftIO $ getStatementColumnNameIndexes ps.handle
   -- tvsth: to make sure ps.handle is not used after release.
   (k3, tvsth) <- lift do
      R.allocate
         (newTMVarIO (Just ps.handle))
         (\tvsth -> atomically $ tryTakeTMVar tvsth >> putTMVar tvsth Nothing)
   Z.untilLeft $ liftIO $ Ex.mask \restore -> Ex.bracket
      (atomically $ takeTMVar tvsth)
      ( -- "try" because the release might have filled `tvsth` with Nothing
        void . atomically . tryPutTMVar tvsth
      )
      \case
         Just sth ->
            restore (bool S.step S.stepNoCB st.unsafeFFI sth) >>= \case
               S.Row -> restore do
                  either Ex.throwM (pure . Right)
                     =<< runOutput
                        (traverse (S.column ps.handle) . flip Map.lookup ixs)
                        st.output
               S.Done -> do
                  liftIO $ traverse_ R.release [k3, k2, k1, k0]
                  pure $ Left ()
         Nothing -> Ex.throwM $ resourceVanishedWithCallStack "Rows"

--------------------------------------------------------------------------------

resourceVanishedWithCallStack :: (HasCallStack) => String -> IOError
resourceVanishedWithCallStack s =
   (userError s)
      { ioe_location = prettyCallStack (popCallStack callStack)
      , ioe_type = ResourceVanished
      }

mapPop :: (Ord k) => k -> Map.Map k v -> (Maybe v, Map.Map k v)
mapPop k m0 | (ml, yv, mr) <- Map.splitLookup k m0 = (yv, ml <> mr)

note :: a -> Maybe b -> Either a b
note a = \case
   Just b -> Right b
   Nothing -> Left a
