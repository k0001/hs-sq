{-# LANGUAGE StrictData #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoFieldSelectors #-}

module Sq.Connection
   ( Connection
   , acquireConnection
   , Transaction
   , Settings (..)
   , defaultSettings
   , acquireTransaction
   , row
   , rowMaybe
   , rowsZero
   , rowsList
   , rowsNonEmpty
   , rowsStream
   , ConnectionId (..)
   , TransactionId (..)
   , LogMessage (..)
   , LogMessageTransaction (..)
   , LogMessageConnection (..)
   , Savepoint
   , savepoint
   , rollbackToSavepoint
   , rollbacking
   , ErrRows (..)
   , ErrStatement (..)
   , defaultLogStderr
   ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Async qualified as Async
import Control.Concurrent.STM
import Control.DeepSeq
import Control.Exception.Safe qualified as Ex
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.Resource qualified as R hiding (runResourceT)
import Control.Monad.Trans.Resource.Extra qualified as R
import Data.Acquire qualified as A
import Data.Bool
import Data.Foldable
import Data.Functor
import Data.IORef
import Data.Int
import Data.List qualified as List
import Data.List.NonEmpty qualified as NEL
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe
import Data.Monoid
import Data.Singletons
import Data.Text qualified as T
import Data.Text.Lazy.Builder qualified as TB
import Data.Text.Lazy.IO qualified as TL
import Data.Time qualified as Time
import Data.Time.Format.ISO8601 qualified as Time
import Data.Tuple
import Data.Word
import Database.SQLite3 qualified as S
import Database.SQLite3.Bindings qualified as S (CDatabase, CStatement)
import Database.SQLite3.Direct qualified as S (Database (..), Statement (..))
import Foreign.C.Types (CInt (..))
import Foreign.Marshal.Alloc (free, malloc)
import Foreign.Ptr (FunPtr, Ptr, freeHaskellFunPtr)
import Foreign.Storable
import GHC.IO (evaluate, unsafeUnmask)
import GHC.Show
import Numeric.Natural
import Streaming qualified as Z
import Streaming.Prelude qualified as Z
import System.Clock qualified as Clock
import System.IO qualified as IO
import System.IO.Unsafe
import Prelude hiding (Read, log)

import Sq.Input
import Sq.Mode
import Sq.Names
import Sq.Statement
import Sq.Support

--------------------------------------------------------------------------------

modeFlags :: Mode -> [S.SQLOpenFlag]
modeFlags = \case
   Read ->
      [ S.SQLOpenReadOnly
      , S.SQLOpenWAL
      , S.SQLOpenNoMutex
      , S.SQLOpenExResCode
      ]
   Write ->
      [ S.SQLOpenReadWrite
      , S.SQLOpenCreate
      , S.SQLOpenWAL
      , S.SQLOpenNoMutex
      , S.SQLOpenExResCode
      ]

--------------------------------------------------------------------------------

data LogMessageConnection = LogMessageConnection
   { id :: ConnectionId
   , mode :: Mode
   }
   deriving stock (Eq, Ord, Show)

data LogMessageTransaction = LogMessageTransaction
   { id :: TransactionId
   , mode :: TransactionMode
   }
   deriving stock (Eq, Ord, Show)

data LogMessage
   = LogMessage
      Time.UTCTime
      T.Text
      ( Maybe
         ( LogMessageConnection
         , Maybe LogMessageTransaction
         )
      )
   deriving stock (Eq, Ord, Show)

--------------------------------------------------------------------------------

data Settings = Settings
   { file :: FilePath
   -- ^ Database file path. Not an URI.
   --
   -- Note: To keep things simple, native @:memory:@ SQLite databases are not
   -- supported. Maybe use @tmpfs@ if you need that?
   , vfs :: S.SQLVFS
   , timeout :: Word32
   -- ^ SQLite Busy Timeout in milliseconds.
   , log :: LogMessage -> IO ()
   }

instance NFData Settings where
   rnf (Settings !_ !_ !_ !_) = ()

instance Show Settings where
   showsPrec n x =
      showParen (n >= appPrec1) $
         showString "Settings {file = "
            . shows x.file
            . showString ", vfs = "
            . shows x.vfs
            . showString ", timeout = "
            . shows x.timeout
            . showString ", log = ..}"

defaultSettings :: FilePath -> Settings
defaultSettings file =
   Settings
      { file
      , vfs = S.SQLVFSDefault
      , timeout = 120_000 -- 2 minutes
      , log = mempty -- defaultLogStderr
      }

--------------------------------------------------------------------------------

-- | A 'Read' or 'Write' connection handle.
--
-- It is safe to attempt to use this connection concurrently without any
-- locking. The 'Connection' itself mantains an internal locking mechanism so
-- that transactions are always executed serially.
--
-- Note: We don't export 'Connection' directly to the public, because it's
-- easier to export just 'Sq.Pool'.
data Connection (mode :: Mode) = Connection
   { id :: ConnectionId
   , log :: Maybe LogMessageTransaction -> T.Text -> IO ()
   , xconn :: TMVar (Maybe (ExclusiveConnection mode))
   -- ^ 'Nothing' if the connection has vanished.
   }

instance NFData (Connection mode) where
   rnf (Connection !_ !_ !_) = ()

instance Show (Connection mode) where
   showsPrec n c =
      showParen (n >= appPrec1) $
         showString "Connection {id = " . shows c.id . showString ", ...}"

acquireConnection
   :: forall mode. (SingI mode) => Settings -> A.Acquire (Connection mode)
acquireConnection s = do
   x <- acquireExclusiveConnection s
   xconn <- R.mkAcquire1 (newTMVarIO (Just x)) \t ->
      atomically $ tryTakeTMVar t >> putTMVar t Nothing
   let lmc = LogMessageConnection{id = x.id, mode = demote @mode}
   pure
      Connection
         { xconn
         , id = x.id
         , log = \yt m -> do
            Ex.handleAsync (\(_ :: Ex.SomeException) -> pure ()) do
               u <- Time.getCurrentTime
               s.log $ LogMessage u m $ Just (lmc, yt)
         }

--------------------------------------------------------------------------------

-- | Internal. While a 'Connection' can be used concurrently, an
-- 'ExclusiveConnection' can't. If one has access to an 'ExclusiveConnection',
-- then one can assume that nobody else has access to the underlying
-- 'S.Database' connection handle at the moment.
data ExclusiveConnection (mode :: Mode) = ExclusiveConnection
   { id :: ConnectionId
   , run :: forall x. (S.Database -> IO x) -> IO x
   , statements :: IORef (Map SQL PreparedStatement)
   }

instance Show (ExclusiveConnection m) where
   showsPrec n c =
      showParen (n >= appPrec1) $
         showString "ExclusiveConnection {id = "
            . shows c.id
            . showString ", ...}"

run :: (MonadIO m) => ExclusiveConnection mode -> (S.Database -> IO x) -> m x
run ExclusiveConnection{run = r} k = liftIO $ r k

--------------------------------------------------------------------------------

lockConnection :: Connection mode -> A.Acquire (ExclusiveConnection mode)
lockConnection c =
   R.mkAcquire1
      ( atomically do
         takeTMVar c.xconn >>= \case
            Just x -> pure x
            Nothing ->
               Ex.throwM $
                  resourceVanishedWithCallStack
                     "lockConnection"
      )
      (atomically . fmap (const ()) . tryPutTMVar c.xconn . Just)

data DatabaseMessage
   = forall x.
      DatabaseMessage
      (S.Database -> IO x)
      (Either Ex.SomeException x -> IO ())

logException :: T.Text -> (T.Text -> IO ()) -> IO a -> IO a
logException prefix log act = Ex.withException act \e ->
   log $ prefix <> ": " <> show' (e :: Ex.SomeException)

acquireExclusiveConnection
   :: forall mode
    . (SingI mode)
   => Settings
   -> A.Acquire (ExclusiveConnection mode)
acquireExclusiveConnection cs = do
   cid :: ConnectionId <- newConnectionId
   dms :: MVar DatabaseMessage <-
      R.mkAcquire1 newEmptyMVar (fmap (const ()) . tryTakeMVar)
   let lmc = LogMessageConnection{id = cid, mode = demote @mode}
       log :: T.Text -> IO ()
       log = \m -> Ex.handleAsync (\(_ :: Ex.SomeException) -> pure ()) do
         u <- Time.getCurrentTime
         cs.log $ LogMessage u m $ Just (lmc, Nothing)
   abackground :: Async.Async () <-
      R.mkAcquire1
         (Async.async (background log (takeMVar dms)))
         Async.uninterruptibleCancel
   liftIO $ Async.link abackground
   statements :: IORef (Map SQL PreparedStatement) <-
      R.mkAcquire1 (newIORef mempty) \r ->
         atomicModifyIORef' r (mempty,) >>= traverse_ \ps ->
            Ex.tryAny (S.finalize ps.handle)
   pure $
      ExclusiveConnection
         { statements
         , id = cid
         , run = \ !act -> do
            mv <- newEmptyMVar
            putMVar dms $! DatabaseMessage act $ putMVar mv
            takeMVar mv >>= either Ex.throwM pure
         }
  where
   background :: forall x. (T.Text -> IO ()) -> IO DatabaseMessage -> IO x
   background log next = R.runResourceT do
      liftIO $ log "Connecting"
      (_, db) <-
         R.allocate
            ( logException "Connecting failed" log do
               S.open2 (T.pack cs.file) (modeFlags (demote @mode)) cs.vfs
            )
            ( \db -> do
               logException "Disconnecting failed" log do
                  Ex.finally
                     (Ex.uninterruptibleMask_ (S.interrupt db))
                     (S.close db)
               log "Disconnected"
            )
      liftIO $ log "Connected"
      setBusyHandler db cs.timeout
      liftIO $
         traverse_
            (S.exec db)
            [ "PRAGMA synchronous=NORMAL"
            , "PRAGMA journal_size_limit=67108864" -- 64 MiB
            , "PRAGMA mmap_size=134217728" -- 128 MiB
            , "PRAGMA cache_size=2000" -- 2000 pages
            ]
      liftIO $ forever do
         DatabaseMessage act res <- next
         Ex.try (unsafeUnmask (act db)) >>= res

--------------------------------------------------------------------------------

-- | See <https://www.sqlite.org/c3ref/busy_handler.html>
foreign import ccall unsafe "sqlite3_busy_handler"
   c_sqlite3_busy_handler
      :: Ptr S.CDatabase
      -> FunPtr (Ptr a -> CInt -> IO CInt)
      -> Ptr a
      -> IO CInt

-- | Returns same as input.
foreign import ccall safe "sqlite3_sleep"
   c_sqlite3_sleep
      :: CInt
      -- ^ milliseconds.
      -> IO CInt

foreign import ccall "wrapper"
   createBusyHandlerPtr
      :: (Ptr Clock.TimeSpec -> CInt -> IO CInt)
      -> IO (FunPtr (Ptr Clock.TimeSpec -> CInt -> IO CInt))

setBusyHandler :: (R.MonadResource m) => S.Database -> Word32 -> m ()
setBusyHandler (S.Database pDB) tmaxMS = do
   (_, pHandler) <- R.allocate (createBusyHandlerPtr handler) freeHaskellFunPtr
   (_, pTimeSpec) <- R.allocate malloc free
   liftIO do
      n <- c_sqlite3_busy_handler pDB pHandler pTimeSpec
      when (n /= 0) do
         Ex.throwString $ "sqlite3_busy_handler: return " <> show n
  where
   tmaxNS :: Integer
   !tmaxNS = fromIntegral tmaxMS * 1_000_000
   handler :: Ptr Clock.TimeSpec -> CInt -> IO CInt
   handler pt0 n = do
      t1 <- Clock.getTime Clock.Monotonic
      t0 <-
         if n /= 0
            then peek pt0
            else poke pt0 t1 $> t1
      if Clock.toNanoSecs (Clock.diffTimeSpec t1 t0) < tmaxNS
         then do
            let ms = ceiling $ logBase 2 (fromIntegral n :: Double)
            c_sqlite3_sleep ms $> 1
         else pure 0

--------------------------------------------------------------------------------

newtype TransactionId = TransactionId Natural
   deriving newtype (Eq, Ord, Show, NFData)

newTransactionId :: (MonadIO m) => m TransactionId
newTransactionId = TransactionId <$> newUnique

newtype ConnectionId = ConnectionId Natural
   deriving newtype (Eq, Ord, Show, NFData)

newConnectionId :: (MonadIO m) => m ConnectionId
newConnectionId = ConnectionId <$> newUnique

--------------------------------------------------------------------------------

-- | A database transaction handle. It's safe to try to use this 'Connection'
-- concurrently.
--
-- While the 'Transaction' is active, an exclusive lock is held on the
-- underlying 'Connection'.
data Transaction (mode :: TransactionMode) = forall cmode.
    (ConnectionSupportsTransaction cmode mode, SingI cmode) =>
   Transaction
   { id :: TransactionId
   , log :: T.Text -> IO ()
   , conn :: Connection cmode
   }

instance NFData (Transaction mode) where
   rnf (Transaction !_ !_ !_) = ()

acquireTransaction
   :: forall tmode cmode
    . (SingI tmode, SingI cmode, ConnectionSupportsTransaction cmode tmode)
   => Connection cmode
   -> A.Acquire (Transaction tmode)
acquireTransaction c@Connection{} = do
   xc <- lockConnection c
   tid <- newTransactionId
   let tmode = demote @tmode :: TransactionMode
       lmt = LogMessageTransaction{id = tid, mode = tmode}
       log (t :: T.Text) = c.log (Just lmt) t
       showe (e :: Ex.SomeException) = show' e
       rollback (ye0 :: Maybe Ex.SomeException) = do
         log $ "Rolling back" <> maybe "" (\e -> " because: " <> showe e) ye0
         logException "Rolling back failed because" log do
            run xc (flip S.exec "ROLLBACK")
         log "Rolled back"
   R.mkAcquireType1
      ( do
         log "Beginning"
         let bmode = if tmode == Reading then "DEFERRED" else "IMMEDIATE"
         logException "Beginning failed because" log do
            run xc (flip S.exec ("BEGIN " <> bmode))
         log "Begun"
      )
      ( const \case
         A.ReleaseExceptionWith e -> rollback (Just e)
         _
            | tmode /= Committing -> rollback Nothing
            | otherwise -> do
               log "Committing"
               logException "Commiting failed because" log do
                  run xc (flip S.exec "COMMIT")
               log "Committed"
      )
   xconn <- R.mkAcquire1 (newTMVarIO (Just xc)) \t ->
      atomically $ tryTakeTMVar t >> putTMVar t Nothing
   pure $
      Transaction
         { log
         , id = tid
         , conn = Connection{id = c.id, log = c.log, xconn}
         }

--------------------------------------------------------------------------------

-- Note: If you have access to a PreparedStatement, you can assume that
-- you are within a Transaction, and that nobody else has access to this
-- PreparedStatement at the moment.
data PreparedStatement = PreparedStatement
   { handle :: S.Statement
   , columns :: Map Name S.ColumnIndex
   , reprepares :: Int
   -- ^ The @SQLITE_STMTSTATUS_REPREPARE@ when @columns@ was generated.
   }

acquirePreparedStatement
   :: SQL
   -> ExclusiveConnection mode
   -> A.Acquire PreparedStatement
acquirePreparedStatement raw xconn =
   R.mkAcquire1
      ( do
         yps <- atomicModifyIORef' xconn.statements \m ->
            swap $ Map.updateLookupWithKey (\_ _ -> Nothing) raw m
         case yps of
            Just ps -> do
               reprepares <- getStatementStatusReprepare ps.handle
               if reprepares == ps.reprepares
                  then pure ps
                  else do
                     columns <- getStatementColumnIndexes ps.handle
                     pure ps{reprepares, columns}
            Nothing -> do
               handle <- run xconn $ flip S.prepare raw.text
               reprepares <- getStatementStatusReprepare handle
               columns <- getStatementColumnIndexes handle
               pure PreparedStatement{handle, reprepares, columns}
      )
      \ps -> flip Ex.onException (S.finalize ps.handle) do
         S.reset ps.handle
         atomicModifyIORef' xconn.statements \m ->
            (Map.insert raw ps m, ())

getStatementStatusReprepare :: S.Statement -> IO Int
getStatementStatusReprepare (S.Statement p) = do
   fromIntegral <$> c_sqlite3_stmt_status p c_SQLITE_STMTSTATUS_REPREPARE 0

-- | See <https://www.sqlite.org/c3ref/stmt_status.html>
foreign import ccall unsafe "sqlite3_stmt_status"
   c_sqlite3_stmt_status
      :: Ptr S.CStatement
      -> CInt
      -- ^ op
      -> CInt
      -- ^ resetFlg
      -> IO CInt

-- | See <https://www.sqlite.org/c3ref/c_stmtstatus_counter.html>
c_SQLITE_STMTSTATUS_REPREPARE :: CInt
c_SQLITE_STMTSTATUS_REPREPARE = 5

getStatementColumnIndexes :: S.Statement -> IO (Map Name S.ColumnIndex)
getStatementColumnIndexes st = do
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
row
   :: (MonadIO m, TransactionCan smode tmode)
   => A.Acquire (Transaction tmode)
   -> Statement smode i o
   -> i
   -> m o
row atx st i = liftIO do
   rowMaybe atx st i >>= \case
      Just o -> pure o
      Nothing -> Ex.throwM ErrRows_TooFew

-- | Like 'rowsList'. Throws 'ErrRows_TooMany' if more than 0 row.
rowsZero
   :: (MonadIO m, TransactionCan smode tmode)
   => A.Acquire (Transaction tmode)
   -> Statement smode i o
   -> i
   -> m ()
rowsZero atx st i = liftIO do
   rowMaybe atx st i >>= \case
      Nothing -> pure ()
      Just _ -> Ex.throwM ErrRows_TooMany

-- | Like 'rowsList'. Throws 'ErrRows_TooMany' if more than 1 row.
rowMaybe
   :: (MonadIO m, TransactionCan smode tmode)
   => A.Acquire (Transaction tmode)
   -> Statement smode i o
   -> i
   -> m (Maybe o)
rowMaybe atx st i = liftIO $ R.runResourceT do
   Z.next (rowsStream atx st i) >>= \case
      Right (o, z1) ->
         Z.next z1 >>= \case
            Left () -> pure (Just o)
            Right _ -> Ex.throwM ErrRows_TooMany
      Left () -> pure Nothing

-- | Like 'rowsList'. Throws 'ErrRows_TooFew' if no rows.
rowsNonEmpty
   :: (MonadIO m, TransactionCan smode tmode)
   => A.Acquire (Transaction tmode)
   -> Statement smode i o
   -> i
   -> m (Int64, NEL.NonEmpty o)
rowsNonEmpty atx st i = liftIO do
   rowsList atx st i >>= \case
      (n, os) | Just nos <- NEL.nonEmpty os -> pure (n, nos)
      _ -> Ex.throwM ErrRows_TooFew

-- | Get the statement output rows as a list, together with its length.
--
-- Holds an exclusive lock on the database connection temporarily, while the
-- list is being constructed.
rowsList
   :: (MonadIO m, TransactionCan smode tmode)
   => A.Acquire (Transaction tmode)
   -> Statement smode i o
   -> i
   -> m (Int64, [o])
rowsList atx st i =
   liftIO
      $ R.runResourceT
      $ Z.fold_
         (\(!n, !acc) o -> (n + 1, acc . (o :)))
         (0, id)
         (fmap ($ []))
      $ rowsStream atx st i

-- | Stream of output rows.
--
-- __An exclusive lock__ will be held on the database while this 'Z.Stream' is
-- being produced.
--
-- __You must exit @m@__, with 'R.runResourceT' or similar, for the transaction
-- lock and related resources to be released.
--
-- __But, as a convenience__, if the 'Z.Stream' is /fully/ consumed, the
-- resources associated with it will be released right away, meaning you can
-- defer exiting @m@ until later.
rowsStream
   :: (R.MonadResource m, TransactionCan smode tmode)
   => A.Acquire (Transaction tmode)
   -> Statement smode i o
   -- ^ If you need the 'Transaction' to be automatically acquired and
   -- released, you may use 'acquireTransactionRead',
   -- 'acquireTransactionCommit', 'acquireTransactionRollback', or
   -- similar here. Otherwise, just use 'pure'.
   -> i
   -> Z.Stream (Z.Of o) m ()
rowsStream atx !st i = do
   binput <- liftIO $ either Ex.throwM evaluate $ runStatementInput st i
   (k, typs) <- lift $ A.allocateAcquire do
      Transaction{conn, log} <- atx
      liftIO $ log $ "Statement: " <> show' st.sql
      xconn <- lockConnection conn
      liftIO $ log $ "Acquiring prepared statement"
      ps <- acquirePreparedStatement st.sql xconn
      liftIO $ log $ "Acquired prepared statement"
      liftIO $ log $ "Binding input"
      R.mkAcquire1 (S.bindNamed ps.handle (runBoundInput binput)) \_ ->
         S.clearBindings ps.handle
      liftIO $ log $ "Bound input"
      typs <- R.mkAcquire1 (newTMVarIO (Just ps)) \typs -> do
         atomically $ tryTakeTMVar typs >> putTMVar typs Nothing
      pure (typs :: TMVar (Maybe PreparedStatement))
   Z.untilLeft $ liftIO $ Ex.mask \restore ->
      Ex.bracket
         ( atomically do
            takeTMVar typs >>= \case
               Just ps -> pure ps
               Nothing -> Ex.throwM $ resourceVanishedWithCallStack "Rows"
         )
         (atomically . fmap (const ()) . tryPutTMVar typs . Just)
         \ps ->
            restore (S.step ps.handle) >>= \case
               S.Done -> Left <$> R.releaseType k A.ReleaseEarly
               S.Row ->
                  either Ex.throwM (pure . Right) =<< restore do
                     runStatementOutput st \n ->
                        traverse (S.column ps.handle) (Map.lookup n ps.columns)

--------------------------------------------------------------------------------


newtype Savepoint = Savepoint {rollback :: IO ()}

savepoint
   :: (MonadIO m, TransactionCan Write tmode)
   => Transaction tmode
   -> m Savepoint
savepoint = \tx -> do
   spId <- newSavepointId
   rowsZero (pure tx) (stSavepoint spId) ()
   pure Savepoint{rollback = rowsZero (pure tx) (stRollbackTo spId) ()}
  where
   stSavepoint :: SavepointId -> Statement Write () ()
   stSavepoint x = statement mempty mempty ("SAVEPOINT '" <> show' x <> "'")
   stRollbackTo :: SavepointId -> Statement Write () ()
   stRollbackTo x = statement mempty mempty ("ROLLBACK TO '" <> show' x <> "'")

rollbackToSavepoint :: (MonadIO m) => Savepoint -> m ()
rollbackToSavepoint s = liftIO s.rollback

-- | All the database changes through the 'Transaction' will be rolled back
-- on release.
--
-- @
-- 'rollbacking' tx = 'void' $ 'R.mkAcquire1' ('savepoint' tx) 'rollbackToSavepoint'
-- @
--
-- You probably want to use 'rollbacking' as follows:
--
-- @
-- 'Sq.with' ('rollbacking' ('pure' tx)) \\() -> do
--     ...
-- @
rollbacking :: (TransactionCan Write tmode) => Transaction tmode -> A.Acquire ()
rollbacking tx = void $ R.mkAcquire1 (savepoint tx) rollbackToSavepoint

{-
savepoint
   :: (TransactionCan Write tmode)
   => Transaction tmode
   -> A.Acquire Savepoint
savepoint tx = do
   spId <- newSavepointId
   mvDone <- R.mkAcquire1 (newMVar False) (void . flip swapMVar True)
   let rollback :: IO () =
         Ex.bracket
            (takeMVar mvDone)
            ( \_ ->
               -- Note that even if `stRollbackTo` fails, we set `mvDone` to
               -- `True` so that `release` doesn't perform `stRelease`, which
               -- would be terrible.
               putMVar mvDone True
            )
            ( \case
               False -> rowsZero (pure tx) stRollbackTo spId
               True -> pure ()
            )
   let release :: IO () = modifyMVar_ mvDone \case
         False -> rowsZero (pure tx) stRelease spId $> True
         True -> pure True
   R.mkAcquireType1
      (rowsZero (pure tx) stSavepoint spId)
      ( const \case
         A.ReleaseExceptionWith _ -> rollback
         _ -> release
      )
   pure Savepoint{rollback}

stRelease :: Statement Write SavepointId ()
stRelease = statement (show >$< "x") mempty "RELEASE $x"
-}

newtype SavepointId = SavepointId Natural
   deriving newtype (Eq, Ord, Show, NFData)

newSavepointId :: (MonadIO m) => m SavepointId
newSavepointId = SavepointId <$> newUnique

--------------------------------------------------------------------------------

_defaultLogStderr_lock :: MVar ()
_defaultLogStderr_lock = unsafePerformIO $ newMVar ()
{-# NOINLINE _defaultLogStderr_lock #-}

defaultLogStderr :: LogMessage -> IO ()
defaultLogStderr (LogMessage u m y) = do
   let !out =
         TB.toLazyText $
            mconcat $
               List.intersperse " " (fmap (\(k, v) -> k <> "=" <> v) attrs)
                  <> [" ", TB.fromText m]
   withMVar _defaultLogStderr_lock \_ ->
      TL.hPutStrLn IO.stderr out
  where
   attrs :: [(TB.Builder, TB.Builder)]
   attrs =
      ("time", TB.fromString (Time.iso8601Show u))
         : do
            (c, yt) <- maybeToList y
            fc c <> (ft =<< maybeToList yt)
   fc :: LogMessageConnection -> [(TB.Builder, TB.Builder)]
   fc x =
      [ ("connection-id", show' x.id)
      , ("connection-mode", TB.fromText (T.toLower (show' x.mode)))
      ]
   ft :: LogMessageTransaction -> [(TB.Builder, TB.Builder)]
   ft x =
      [ ("transaction-id", show' x.id)
      , ("transaction-mode", TB.fromText (T.toLower (show' x.mode)))
      ]
