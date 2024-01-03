{-# LANGUAGE StrictData #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoFieldSelectors #-}
{-# OPTIONS_HADDOCK hide #-}

module Sq.Internal where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Async qualified as Async
import Control.Concurrent.STM
import Control.Exception.Safe qualified as Ex
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.Reader
import Control.Monad.Trans.Resource qualified as R hiding (runResourceT)
import Control.Monad.Trans.Resource.Extra qualified as R
import Control.Retry qualified as Retry
import Data.Acquire qualified as A
import Data.Bifunctor
import Data.Bool
import Data.Char qualified as Ch
import Data.Coerce
import Data.Foldable
import Data.Functor.Contravariant
import Data.Functor.Contravariant.Divisible
import Data.IORef
import Data.Int
import Data.List.NonEmpty qualified as NEL
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Monoid
import Data.Profunctor
import Data.String
import Data.Text qualified as T
import Data.Void
import Database.SQLite3 qualified as S
import GHC.IO (unsafeUnmask)
import GHC.IO.Exception
import GHC.Show
import GHC.Stack
import Language.Haskell.TH.Quote (QuasiQuoter (..))
import Numeric.Natural
import Streaming qualified as Z
import Streaming.Prelude qualified as Z
import System.IO.Unsafe
import Prelude hiding (log)

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

void :: Input Void
void = Input absurd

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

--------------------------------------------------------------------------------

newtype BoundInput = BoundInput (Map BindingName S.SQLData)
   deriving newtype (Eq, Show)

bindInput :: Input i -> i -> Either ErrBinding BoundInput
bindInput ii i = fmap BoundInput do
   Map.traverseWithKey
      ( \bn -> \case
         Right !d -> Right d
         Left e -> Left $ ErrBinding bn e
      )
      (runInput ii i)

bindRawStatement :: S.Statement -> BoundInput -> A.Acquire ()
bindRawStatement sth (BoundInput m) = do
   let !raw = first renderBindingName <$> Map.toAscList m
   R.mkAcquire1 (S.bindNamed sth raw) (\_ -> S.clearBindings sth)

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

newtype RawStatement = RawStatement T.Text
   deriving newtype (Eq, Ord, Show, IsString)

unRawStatement :: RawStatement -> T.Text
unRawStatement = coerce

rawStatement :: QuasiQuoter
rawStatement =
   QuasiQuoter
      { quoteExp = \s -> [|fromString @RawStatement s|]
      , quotePat = \_ -> fail "rawStatement: No quotePat"
      , quoteType = \_ -> fail "rawStatement: No quoteType"
      , quoteDec = \_ -> fail "rawStatement: No quoteDec"
      }

--------------------------------------------------------------------------------

data Settings = Settings
   { database :: T.Text
   , flags :: [S.SQLOpenFlag]
   , vfs :: S.SQLVFS
   , log :: ConnectionId -> Maybe TransactionId -> String -> IO ()
   }

log
   :: (MonadIO m)
   => Settings
   -> ConnectionId
   -> Maybe TransactionId
   -> String
   -> m ()
log s cid ytid msg = liftIO $ Ex.catchAny (s.log cid ytid msg) mempty

instance Show Settings where
   showsPrec n x =
      showParen (n >= appPrec1) $
         showString "Settings {database = "
            . shows x.database
            . showString ", flags = "
            . shows x.flags
            . showString ", vfs = "
            . shows x.vfs
            . showString ", ...}"

--------------------------------------------------------------------------------

data Connection = Connection
   { id :: ConnectionId
   , log :: Maybe TransactionId -> String -> IO ()
   , xconn :: TMVar (Maybe ExclusiveConnection)
   }

instance Show Connection where
   showsPrec n c =
      showParen (n >= appPrec1) $
         showString "Connection {id = " . shows c.id . showString ", ...}"

acquireConnection :: Settings -> A.Acquire Connection
acquireConnection s = do
   x <- acquireExclusiveConnection s
   xconn <- R.mkAcquire1 (newTMVarIO (Just x)) \t ->
      atomically $ tryTakeTMVar t >> putTMVar t Nothing
   pure Connection{xconn, id = x.id, log = s.log x.id}

--------------------------------------------------------------------------------

-- | A database connection handle. It's safe to try to use this 'Connection'
-- concurrently.
data ExclusiveConnection = ExclusiveConnection
   { id :: ConnectionId
   , run :: forall x. (S.Database -> IO x) -> IO x
   , statements :: IORef (Map RawStatement PreparedStatement)
   }

instance Show ExclusiveConnection where
   showsPrec n c =
      showParen (n >= appPrec1) $
         showString "ExclusiveConnection {id = "
            . shows c.id
            . showString ", ...}"

run :: (MonadIO m) => ExclusiveConnection -> (S.Database -> IO x) -> m x
run ExclusiveConnection{run = r} k = liftIO $ r (retrySqlBusy . k)

retrySqlBusy :: IO a -> IO a
retrySqlBusy = \ioa ->
   Retry.recovering
      ( -- TODO log after a bunch of retries
        Retry.constantDelay 50_000 {- 50 ms single retry -}
      )
      [\_ -> Ex.Handler \e -> pure (S.sqlError e == S.ErrorBusy)]
      (\_ -> ioa)

--------------------------------------------------------------------------------

acquireExclusiveConnectionLock :: Connection -> A.Acquire ExclusiveConnection
acquireExclusiveConnectionLock c =
   R.mkAcquire1
      ( atomically do
         takeTMVar c.xconn >>= \case
            Just x -> pure x
            Nothing ->
               Ex.throwM $
                  resourceVanishedWithCallStack
                     "acquireExclusiveConnectionLock"
      )
      (atomically . fmap (const ()) . tryPutTMVar c.xconn . Just)

data DatabaseMessage
   = forall x.
      DatabaseMessage
      (S.Database -> IO x)
      (Either Ex.SomeException x -> IO ())

acquireExclusiveConnection :: Settings -> A.Acquire ExclusiveConnection
acquireExclusiveConnection s = do
   cid :: ConnectionId <- newConnectionId
   statements :: IORef (Map RawStatement PreparedStatement) <-
      R.mkAcquire1 (newIORef mempty) \r ->
         atomicModifyIORef' r (mempty,) >>= traverse_ \ps ->
            Ex.catchAsync @_ @Ex.SomeException (S.finalize ps.handle) mempty
   dms :: MVar DatabaseMessage <- liftIO newEmptyMVar
   abackground :: Async.Async () <-
      R.mkAcquire1
         (Async.async (background (takeMVar dms)))
         Async.uninterruptibleCancel
   liftIO $ Async.link abackground
   pure $
      ExclusiveConnection
         { statements
         , id = cid
         , run = \act -> do
            mv <- newEmptyMVar
            putMVar dms $! DatabaseMessage act $ putMVar mv
            takeMVar mv >>= either Ex.throwM pure
         }
  where
   background :: forall x. IO DatabaseMessage -> IO x
   background next =
      Ex.bracket
         (S.open2 s.database s.flags s.vfs)
         (\h -> Ex.uninterruptibleMask_ (S.interrupt h) `Ex.finally` S.close h)
         \h -> do
            S.exec h "PRAGMA busy_timeout=30000" -- 30 seconds
            forever do
               DatabaseMessage act res <- next
               Ex.try (unsafeUnmask (act h)) >>= res

--------------------------------------------------------------------------------

iorefUnique :: IORef Natural
iorefUnique = unsafePerformIO (newIORef 0)
{-# NOINLINE iorefUnique #-}

newUnique :: (MonadIO m) => m Natural
newUnique = liftIO $ atomicModifyIORef' iorefUnique \n -> (n + 1, n)

newtype TransactionId = TransactionId Natural
   deriving newtype (Eq, Ord, Show)

newTransactionId :: (MonadIO m) => m TransactionId
newTransactionId = TransactionId <$> newUnique

newtype ConnectionId = ConnectionId Natural
   deriving newtype (Eq, Ord, Show)

newConnectionId :: (MonadIO m) => m ConnectionId
newConnectionId = ConnectionId <$> newUnique

--------------------------------------------------------------------------------

-- | A database transaction handle. It's safe to try to use this 'Connection'
-- concurrently.
--
-- While the 'Transaction' is active, an exclusive lock is held on the
-- underlying 'Connection'.
data Transaction = Transaction
   { id :: TransactionId
   , log :: String -> IO ()
   , connection :: Connection
   }

-- | @BEGIN@s a database transaction. If released with 'A.ReleaseExceptionWith',
-- then the transaction is @ROLLBACK@ed. Otherwise, it is @COMMIT@ed.
acquireCommittingTransaction :: Connection -> A.Acquire Transaction
acquireCommittingTransaction c = do
   xc <- acquireExclusiveConnectionLock c
   tid <- newTransactionId
   let tlog = c.log (Just tid)
   R.mkAcquireType1
      ( do
         Ex.withException
            (run xc (flip S.exec "BEGIN IMMEDIATE"))
            \(e :: Ex.SomeException) ->
               tlog $ "BEGIN comitting transaction failed: " <> show e
         tlog $ "BEGIN comitting transaction OK"
      )
      ( const \case
         A.ReleaseExceptionWith e0 -> do
            let pre = "ROLLBACK (" <> show e0 <> ")"
            Ex.withException (run xc (flip S.exec "ROLLBACK")) \e1 ->
               tlog $ pre <> " failed: " <> show (e1 :: Ex.SomeException)
            tlog $ pre <> " OK"
         _ -> run xc (flip S.exec "COMMIT") <* tlog "COMMIT"
      )
   xconn <- R.mkAcquire1 (newTMVarIO (Just xc)) \t ->
      atomically $ tryTakeTMVar t >> putTMVar t Nothing
   pure $
      Transaction
         { log = tlog
         , id = tid
         , connection = Connection{id = c.id, log = c.log, xconn}
         }

-- | @BEGIN@s a database transaction which will be @ROLLBACK@ed when released.
acquireRollbackingTransaction :: Connection -> A.Acquire Transaction
acquireRollbackingTransaction c = do
   xc <- acquireExclusiveConnectionLock c
   tid <- newTransactionId
   let tlog = c.log (Just tid)
   R.mkAcquireType1
      ( do
         Ex.withException
            (run xc (flip S.exec "BEGIN DEFERRED"))
            \(e :: Ex.SomeException) ->
               tlog $ "BEGIN rollbacking transaction failed: " <> show e
         tlog $ "BEGIN rollbacking transaction OK"
      )
      ( const \case
         A.ReleaseExceptionWith e -> do
            run xc (flip S.exec "ROLLBACK")
            tlog ("ROLLBACK (" <> show e <> ")")
         _ -> run xc (flip S.exec "ROLLBACK") <* tlog "ROLLBACK"
      )
   xconn <- R.mkAcquire1 (newTMVarIO (Just xc)) \t ->
      atomically $ tryTakeTMVar t >> putTMVar t Nothing
   pure $
      Transaction
         { log = tlog
         , id = tid
         , connection = Connection{id = c.id, log = c.log, xconn}
         }

--------------------------------------------------------------------------------

data BoundStatement o = BoundStatement
   { safeFFI :: Bool
   , input :: BoundInput
   , output :: Output o
   , raw :: RawStatement
   }
   deriving stock (Functor)

instance Show (BoundStatement o) where
   showsPrec n s =
      showParen (n >= appPrec1) $
         showString "BoundStatement{raw = "
            . shows s.raw
            . showString ", safeFFI = "
            . shows s.safeFFI
            . showString ", input = "
            . shows s.input
            . showString "}"

bindStatement :: Statement i o -> i -> Either ErrBinding (BoundStatement o)
bindStatement s@Statement{safeFFI, output, raw} i = do
   input <- bindInput s.input i
   pure BoundStatement{..}

--------------------------------------------------------------------------------

-- | A statements statement taking a value @i@ as input and producing rows of
-- @o@ values as output.
data Statement i o = Statement
   { safeFFI :: Bool
   , input :: Input i
   , output :: Output o
   , raw :: RawStatement
   }

instance Show (Statement i o) where
   showsPrec n s =
      showParen (n >= appPrec1) $
         showString "Statement{raw = "
            . shows s.raw
            . showString ", safeFFI = "
            . shows s.safeFFI
            . showString "}"

statement :: Input i -> Output o -> RawStatement -> Statement i o
statement input output raw = Statement{safeFFI = True, ..}

instance Functor (Statement i) where
   fmap = rmap

instance Profunctor Statement where
   dimap f g Statement{..} =
      Statement{input = f >$< input, output = g <$> output, ..}

--------------------------------------------------------------------------------

data PreparedStatement = PreparedStatement
   { handle :: S.Statement
   , safeFFI :: Bool
   }

step :: PreparedStatement -> IO S.StepResult
step p
   | p.safeFFI = retrySqlBusy $ S.step p.handle
   | otherwise = retrySqlBusy $ S.stepNoCB p.handle

acquirePreparedStatement
   :: RawStatement
   -> Bool
   -- ^ safeFFI
   -> ExclusiveConnection
   -> A.Acquire PreparedStatement
acquirePreparedStatement rst safeFFI xconn =
   R.mkAcquire1
      ( do
         yps <- atomicModifyIORef' xconn.statements \m ->
            case Map.splitLookup rst m of
               (ml, yps, mr) -> (ml <> mr, yps)
         case yps of
            Just ps -> pure ps
            Nothing -> do
               handle <- run xconn $ flip S.prepare (unRawStatement rst)
               let ps = PreparedStatement{handle, safeFFI}
               atomicModifyIORef' xconn.statements \m ->
                  (Map.insert rst ps m, ps)
      )
      \ps -> flip Ex.onException (S.finalize ps.handle) do
         S.reset ps.handle
         atomicModifyIORef' xconn.statements \m ->
            (Map.insert rst ps m, ())

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
row :: (MonadIO m) => BoundStatement o -> Transaction -> m o
row st tx = liftIO do
   rowMaybe st tx >>= \case
      Just o -> pure o
      Nothing -> Ex.throwM ErrRows_TooFew

-- | Like 'rowsList'. Throws 'ErrRows_TooMany' if more than 1 row.
rowMaybe :: (MonadIO m) => BoundStatement o -> Transaction -> m (Maybe o)
rowMaybe st tx = liftIO $ R.runResourceT do
   Z.next (rowsStream st (pure tx)) >>= \case
      Right (o, z1) ->
         Z.next z1 >>= \case
            Left () -> pure (Just o)
            Right _ -> Ex.throwM ErrRows_TooMany
      Left () -> pure Nothing

-- | Like 'rowsList'. Throws 'ErrRows_TooFew' if no rows.
rowsNonEmpty
   :: (MonadIO m)
   => BoundStatement o
   -> Transaction
   -> m (Int64, NEL.NonEmpty o)
rowsNonEmpty st tx = liftIO do
   rowsList st tx >>= \case
      (n, os) | Just nos <- NEL.nonEmpty os -> pure (n, nos)
      _ -> Ex.throwM ErrRows_TooFew

-- | Get the statement output rows as a list, together with its length.
--
-- Holds an exclusive lock on the database connection temporarily, while the
-- list is being constructed.
rowsList :: (MonadIO m) => BoundStatement o -> Transaction -> m (Int64, [o])
rowsList st tx =
   liftIO
      $ R.runResourceT
      $ Z.fold_
         (\(!n, !e) o -> (n + 1, e <> Endo (o :)))
         (0, mempty)
         (fmap (flip appEndo []))
      $ rowsStream st (pure tx)

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
   => BoundStatement o
   -> A.Acquire Transaction
   -> Z.Stream (Z.Of o) m ()
rowsStream st atx = do
   (k, (ixs, typs)) <- lift $ A.allocateAcquire do
      tx <- atx
      xc <- acquireExclusiveConnectionLock tx.connection
      ps <- acquirePreparedStatement st.raw st.safeFFI xc
      ixs <- liftIO $ getStatementColumnNameIndexes ps.handle
      bindRawStatement ps.handle st.input
      typs <- R.mkAcquire1 (newTMVarIO (Just ps)) \typs -> do
         atomically $ tryTakeTMVar typs >> putTMVar typs Nothing
      pure (ixs, typs)
   Z.untilLeft $ liftIO $ Ex.mask \restore ->
      Ex.bracket
         ( atomically do
            takeTMVar typs >>= \case
               Just ps -> pure ps
               Nothing -> Ex.throwM $ resourceVanishedWithCallStack "Rows"
         )
         (atomically . fmap (const ()) . tryPutTMVar typs . Just)
         \ps ->
            restore (step ps) >>= \case
               S.Done -> Left <$> R.releaseType k A.ReleaseEarly
               S.Row ->
                  either Ex.throwM (pure . Right) =<< restore do
                     runOutput
                        (traverse (S.column ps.handle) . flip Map.lookup ixs)
                        st.output

--------------------------------------------------------------------------------

resourceVanishedWithCallStack :: (HasCallStack) => String -> IOError
resourceVanishedWithCallStack s =
   (userError s)
      { ioe_location = prettyCallStack (popCallStack callStack)
      , ioe_type = ResourceVanished
      }

note :: a -> Maybe b -> Either a b
note a = \case
   Just b -> Right b
   Nothing -> Left a
