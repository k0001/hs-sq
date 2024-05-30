{-# LANGUAGE StrictData #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoFieldSelectors #-}

module Sq.Connection
   ( Connection
   , connection
   , Transaction (smode)
   , Settings (..)
   , settings
   , connectionReadTransaction
   , connectionWriteTransaction
   , foldIO
   , streamIO
   , ConnectionId (..)
   , TransactionId (..)
   , SavepointId (..)
   , Savepoint
   , savepoint
   , savepointRollback
   , savepointRelease
   , ErrRows (..)
   , ErrStatement (..)
   , ErrTransaction (..)
   ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.DeepSeq
import Control.Exception.Safe qualified as Ex
import Control.Foldl qualified as F
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Class
import Control.Monad.Trans.Resource qualified as R hiding (runResourceT)
import Control.Monad.Trans.Resource.Extra qualified as R
import Data.Acquire qualified as A
import Data.Foldable
import Data.Function (fix)
import Data.Functor
import Data.IORef
import Data.Int
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe
import Data.Monoid
import Data.Text qualified as T
import Data.Tuple
import Data.Word
import Database.SQLite3 qualified as S
import Database.SQLite3.Bindings qualified as S (CDatabase, CStatement)
import Database.SQLite3.Direct qualified as S (Database (..), Statement (..))
import Di.Df1 qualified as Di
import Foreign.C.Types (CInt (..))
import Foreign.Marshal.Alloc (free, malloc)
import Foreign.Ptr (FunPtr, Ptr, freeHaskellFunPtr, nullFunPtr, nullPtr)
import Foreign.Storable
import GHC.Records
import GHC.Show
import Streaming qualified as Z
import Streaming.Prelude qualified as Z
import System.Clock qualified as Clock
import System.Timeout (timeout)
import Prelude hiding (Read, log)

import Sq.Input
import Sq.Mode
import Sq.Names
import Sq.Output
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

-- | SQLite connection settings.
data Settings = Settings
   { file :: FilePath
   -- ^ Database file path. Not an URI.
   --
   -- Note: To keep things simple, native @:memory:@ SQLite databases are not
   -- supported. Maybe use 'Sq.poolTemp' or @tmpfs@ if you need that?
   , vfs :: S.SQLVFS
   , timeout :: Word32
   -- ^ SQLite busy Timeout in milliseconds.
   }
   deriving stock (Eq, Show)

instance NFData Settings where
   rnf (Settings !_ !_ !_) = ()

-- | Default connection settings.
settings
   :: FilePath
   -- ^ Database file path. Not an URI, not @:memory:@
   -> Settings
settings file =
   Settings
      { file
      , vfs = S.SQLVFSDefault
      , timeout = 120_000 -- 2 minutes
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
data Connection (c :: Mode) = Connection
   { _id :: ConnectionId
   , timeout :: Word32
   -- ^ Same @timeout@ as in 'Settings'
   , di :: Di.Df1
   , xconn :: TMVar (Maybe (ExclusiveConnection c))
   -- ^ 'Nothing' if the connection has vanished.
   }

instance HasField "id" (Connection c) ConnectionId where
   getField = (._id)

instance NFData (Connection c) where
   rnf (Connection !_ !_ !_ !_) = ()

instance Show (Connection c) where
   showsPrec _ c = showString "Connection{id = " . shows c.id . showChar '}'

connection :: SMode mode -> Di.Df1 -> Settings -> A.Acquire (Connection c)
connection smode di0 s = do
   (di1, xc) <- exclusiveConnection smode di0 s
   xconn <- R.mkAcquire1 (newTMVarIO (Just xc)) \t ->
      atomically $ tryTakeTMVar t >> putTMVar t Nothing
   pure Connection{xconn, _id = xc.id, di = di1, timeout = s.timeout}

--------------------------------------------------------------------------------

-- | Internal. While a 'Connection' can be used concurrently, an
-- 'ExclusiveConnection' can't. If one has access to an 'ExclusiveConnection',
-- then one can assume that nobody else has access to the underlying
-- 'S.Database' connection handle at the moment.
data ExclusiveConnection (mode :: Mode) = ExclusiveConnection
   { id :: ConnectionId
   , txint :: IORef (Maybe Bool)
   -- ^ 'Nothing' if no transaction has started,
   -- 'True' if the previously started transaction was interrupted.
   , run :: forall x. (S.Database -> IO x) -> IO x
   , statements :: IORef (Map SQL PreparedStatement)
   }

instance Show (ExclusiveConnection m) where
   showsPrec _ x =
      showString "ExclusiveConnection{id = " . shows x.id . showChar '}'

run :: (MonadIO m) => ExclusiveConnection c -> (S.Database -> IO x) -> m x
run ExclusiveConnection{run = r} k = liftIO $ r k

--------------------------------------------------------------------------------

lockConnection :: Connection c -> A.Acquire (ExclusiveConnection c)
lockConnection c =
   R.mkAcquire1
      ( warningOnException (Di.push "lock" c.di) do
         -- We reuse setBusyHandler's timeout because why not.
         y <- timeout (fromIntegral c.timeout * 1000) $ atomically do
            takeTMVar c.xconn >>= \case
               Just x -> pure x
               Nothing ->
                  Ex.throwM $
                     resourceVanishedWithCallStack
                        "lockConnection"
         case y of
            Just xc -> pure xc
            Nothing -> Ex.throwString "Timeout"
      )
      (atomically . void . tryPutTMVar c.xconn . Just)

warningOnException
   :: (MonadIO m, Ex.MonadMask m)
   => Di.Df1
   -> m a
   -> m a
warningOnException di act = Ex.withException act \e ->
   Di.warning di (e :: Ex.SomeException)

exclusiveConnection
   :: SMode mode
   -> Di.Df1
   -> Settings
   -> A.Acquire (Di.Df1, ExclusiveConnection c)
exclusiveConnection smode di0 cs = do
   cId :: ConnectionId <- newConnectionId
   let di1 = Di.attr "connection-mode" smode $ Di.attr "connection" cId di0
   db :: S.Database <-
      R.mkAcquire1
         ( do
            let di2 = Di.push "connect" di1
            db <- warningOnException di2 do
               S.open2 (T.pack cs.file) (modeFlags (fromSMode smode)) cs.vfs
            Di.debug_ di2 "OK"
            pure db
         )
         ( \db -> do
            let di2 = Di.push "disconnect" di1
            warningOnException di1 $ S.close db
            Di.debug_ di2 "OK"
         )
   setBusyHandler db cs.timeout
   liftIO $
      traverse_
         (S.exec db)
         [ "PRAGMA synchronous=NORMAL"
         , "PRAGMA journal_size_limit=67108864" -- 64 MiB
         , "PRAGMA mmap_size=134217728" -- 128 MiB
         , "PRAGMA cache_size=2000" -- 2000 pages
         ]
   statements :: IORef (Map SQL PreparedStatement) <-
      R.mkAcquire1 (newIORef mempty) \r ->
         atomicModifyIORef' r (mempty,) >>= traverse_ \ps ->
            Ex.tryAny (S.finalize ps.handle)
   txint <- liftIO $ newIORef Nothing
   runlock <- R.mkAcquire1 (newMVar ()) takeMVar
   pure
      ( di1
      , ExclusiveConnection
         { statements
         , txint
         , id = cId
         , run = \act -> withMVar runlock \() -> Ex.mask \restore -> do
            mx <- newEmptyMVar
            th <- forkIO $ Ex.tryAsync (restore (act db)) >>= putMVar mx
            Ex.tryAsync (takeMVar mx) >>= \case
               Right (Right x) -> pure x
               Right (Left (se :: Ex.SomeException)) -> Ex.throwM se
               Left (se :: Ex.SomeException) -> do
                  when (Ex.isAsyncException se) do
                     Ex.catchAsync
                        ( Ex.uninterruptibleMask_ do
                           -- We deal with txint later during Transaction release
                           atomicModifyIORef' txint \case
                              Nothing -> (Nothing, ())
                              Just _ -> (Just True, ())
                           S.interrupt db
                           killThread th
                           void $ takeMVar mx
                        )
                        (\(_ :: Ex.SomeException) -> pure ())
                  Ex.throwM se
         }
      )

--------------------------------------------------------------------------------

-- | See <https://www.sqlite.org/c3ref/busy_handler.html>
foreign import ccall unsafe "sqlite3_busy_handler"
   c_sqlite3_busy_handler
      :: Ptr S.CDatabase
      -> FunPtr (Ptr a -> CInt -> IO CInt)
      -> Ptr a
      -> IO CInt

c_sqlite3_busy_handler'
   :: Ptr S.CDatabase
   -> FunPtr (Ptr a -> CInt -> IO CInt)
   -> Ptr a
   -> IO ()
c_sqlite3_busy_handler' pDB pF pX = do
   n <- c_sqlite3_busy_handler pDB pF pX
   when (n /= 0) do
      Ex.throwString $ "sqlite3_busy_handler: return " <> show n

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

setBusyHandler :: S.Database -> Word32 -> A.Acquire ()
setBusyHandler (S.Database pDB) tmaxMS = do
   pHandler <- R.mkAcquire1 (createBusyHandlerPtr handler) freeHaskellFunPtr
   pTimeSpec <- R.mkAcquire1 malloc free
   R.mkAcquire1
      (c_sqlite3_busy_handler' pDB pHandler pTimeSpec)
      (\_ -> c_sqlite3_busy_handler' pDB nullFunPtr nullPtr)
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
            let ms = ceiling $ logBase 2 (fromIntegral (max 1 n) :: Double)
            c_sqlite3_sleep ms $> 1
         else pure 0

--------------------------------------------------------------------------------

-- | A database transaction handle.
--
-- * @t@ indicates whether 'Read'-only or read-'Write' 'Statement's are
-- supported.
--
-- * Prefer to use a 'Read'-only 'Transaction' if you are solely performing
-- 'Read'-only 'Statement's. It will be more efficient in concurrent settings.
--
-- * Obtain with 'Sq.readTransaction' or 'Sq.commitTransaction'. Or, if you
-- are testing, with 'Sq.rollbackTransaction'.
--
-- * If you have access to a 'Transaction' within its intended scope, then you
-- can assume that a database transaction has started, and will eventually be
-- automatically commited or rolled back as requested when it was obtained.
--
-- * It's safe and efficient to use a 'Transaction' concurrently as is.
-- Concurrency is handled internally.

-- While the 'Transaction' is active, an exclusive lock is held on the
-- underlying 'Connection'.
data Transaction (t :: Mode) = forall c.
    (SubMode c t) =>
   Transaction
   { _id :: TransactionId
   , di :: Di.Df1
   , conn :: Connection c
   , commit :: Bool
   , smode :: SMode t
   }

instance Show (Transaction t) where
   showsPrec _ t =
      showString "Transaction{id = "
         . shows t.id
         . showString ", commit = "
         . shows t.commit
         . showChar '}'

instance NFData (Transaction t) where
   rnf (Transaction !_ !_ !_ !_ !_) = ()

instance HasField "id" (Transaction t) TransactionId where
   getField = (._id)

connectionReadTransaction
   :: (SubMode c Read)
   => Connection c
   -> A.Acquire (Transaction 'Read)
connectionReadTransaction c = do
   xc <- lockConnection c
   tId <- newTransactionId
   let di1 = Di.attr "transaction-mode" Read $ Di.attr "transaction" tId c.di
   R.mkAcquireType1
      ( do
         let di2 = Di.push "begin" di1
         warningOnException di2 do
            readIORef xc.txint >>= \case
               Nothing -> pure ()
               _ -> Ex.throwString "Nested transaction. Should never happen."
            run xc (flip S.exec "BEGIN DEFERRED")
            atomicWriteIORef xc.txint $ Just False
            Di.debug_ di2 "OK"
      )
      ( \_ rt -> do
         let di2 = Di.push "rollback" di1
         warningOnException di2 do
            readIORef xc.txint >>= \case
               Just False -> do
                  for_ (releaseTypeException rt) \e ->
                     Di.notice di2 $ "Will rollback due to: " <> show e
                  run xc (flip S.exec "ROLLBACK")
                  atomicWriteIORef xc.txint Nothing
                  Di.debug_ di2 "OK"
               Just True -> do
                  atomicWriteIORef xc.txint Nothing
                  Di.info_ di2 "Previously interrupted, no need to rollback"
               Nothing ->
                  Ex.throwString "No transaction. Should never happen."
      )
   xconn <- R.mkAcquire1 (newTMVarIO (Just xc)) \t ->
      atomically $ tryTakeTMVar t >> putTMVar t Nothing
   pure $
      Transaction
         { _id = tId
         , di = di1
         , conn = c{xconn}
         , commit = False
         , smode = SRead
         }

connectionWriteTransaction
   :: Bool
   -- ^ Whether to finally @COMMIT@ the transaction.
   -- Otherwise, it will @ROLLBACK@.
   -> Connection 'Write
   -> A.Acquire (Transaction 'Write)
connectionWriteTransaction commit c = do
   xc <- lockConnection c
   tId <- newTransactionId
   let di1 =
         Di.attr_ "transaction-mode" (if commit then "commit" else "rollback") $
            Di.attr "transaction" tId c.di
       rollback (ye :: Maybe Ex.SomeException) = do
         let di2 = Di.push "rollback" di1
         warningOnException di2 do
            readIORef xc.txint >>= \case
               Just False -> do
                  for_ ye \e ->
                     Di.notice di2 $ "Will rollback due to: " <> show e
                  run xc (flip S.exec "ROLLBACK")
                  atomicWriteIORef xc.txint Nothing
                  Di.debug_ di2 "OK"
               Just True -> do
                  atomicWriteIORef xc.txint Nothing
                  Di.info_ di2 "Previously interrupted, no need to rollback"
               Nothing ->
                  Ex.throwString "No transaction. Should never happen."
   R.mkAcquireType1
      ( do
         let di2 = Di.push "begin" di1
         warningOnException di2 do
            readIORef xc.txint >>= \case
               Nothing -> pure ()
               _ -> Ex.throwString "Nested transaction. Should never happen."
            run xc (flip S.exec "BEGIN IMMEDIATE")
            atomicWriteIORef xc.txint $ Just False
            Di.debug_ di2 "OK"
      )
      ( \_ rt -> case releaseTypeException rt of
         Nothing
            | commit -> do
               let di2 = Di.push "commit" di1
               warningOnException di2 do
                  readIORef xc.txint >>= \case
                     Just False -> do
                        run xc (flip S.exec "COMMIT")
                        atomicWriteIORef xc.txint Nothing
                        Di.debug_ di2 "OK"
                     Just True -> Ex.throwM ErrTransaction_Interrupted
                     Nothing ->
                        Ex.throwString "No transaction. Should never happen."
            | otherwise -> rollback Nothing
         Just e -> rollback (Just e)
      )
   xconn <- R.mkAcquire1 (newTMVarIO (Just xc)) \t ->
      atomically $ tryTakeTMVar t >> putTMVar t Nothing
   pure $
      Transaction
         { _id = tId
         , di = di1
         , conn = c{xconn}
         , commit
         , smode = SWrite
         }

data ErrTransaction
   = -- | A the database connection was 'S.interrupt'ed but a @COMMIT@ is still
     -- pending.
     --
     -- This exception happens if you swallowed an asynchronous exception
     -- before releasing a 'Write' 'Transaction'. Just don't do that.  Make
     -- sure the 'Transaction' is released through 'A.ReleaseExceptionWith'.
     ErrTransaction_Interrupted
   deriving stock (Eq, Show)
   deriving anyclass (Ex.Exception)

--------------------------------------------------------------------------------

-- Note: If you have access to a PreparedStatement, you can assume that
-- you are within a Transaction, and that nobody else has access to this
-- PreparedStatement at the moment.
data PreparedStatement = PreparedStatement
   { handle :: S.Statement
   , columns :: Map BindingName S.ColumnIndex
   , id :: StatementId
   , reprepares :: Int
   -- ^ The @SQLITE_STMTSTATUS_REPREPARE@ when @columns@ was generated.
   }

acquirePreparedStatement
   :: Di.Df1
   -> SQL
   -> ExclusiveConnection c
   -> A.Acquire PreparedStatement
acquirePreparedStatement di0 raw xconn = R.mkAcquire1
   ( do
      yps <- atomicModifyIORef' xconn.statements \m ->
         swap $ Map.updateLookupWithKey (\_ _ -> Nothing) raw m
      case yps of
         Just ps -> do
            reprepares <- getStatementStatusReprepare ps.handle
            if reprepares == ps.reprepares
               then pure ps
               else do
                  let di1 = Di.attr "stmt" ps.id di0
                  Di.debug_ di1 "Reprepared"
                  columns <- getStatementColumnIndexes ps.handle
                  Di.debug di1 $ "Columns: " <> show (Map.toAscList columns)
                  pure ps{reprepares, columns}
         Nothing -> do
            stId <- newStatementId
            let di1 = Di.attr "stmt" stId di0
            Di.debug di1 $ "Preparing " <> show raw
            handle <- run xconn $ flip S.prepare raw.text
            reprepares <- getStatementStatusReprepare handle
            columns <- getStatementColumnIndexes handle
            Di.debug di1 $ "Columns: " <> show (Map.toAscList columns)
            pure PreparedStatement{id = stId, handle, reprepares, columns}
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

getStatementColumnIndexes :: S.Statement -> IO (Map BindingName S.ColumnIndex)
getStatementColumnIndexes st = do
   -- Despite the type name, ncols is a length.
   S.ColumnIndex (ncols :: Int) <- S.columnCount st
   Control.Monad.foldM
      ( \ !m i -> do
         -- Pattern never fails because `i` is in range.
         Just t <- S.columnName st i
         case parseOutputBindingName t of
            Right n ->
               Map.alterF
                  ( \case
                     Nothing -> pure $ Just i
                     Just _ -> Ex.throwM $ ErrStatement_DuplicateColumnName n
                  )
                  n
                  m
            Left _ ->
               -- If `t` is not binding name as understood by
               -- `parseOutputBindingName`, we ignore it.
               -- It just won't be available for lookup later on.
               pure m
      )
      Map.empty
      (S.ColumnIndex <$> enumFromTo 0 (ncols - 1))

data ErrStatement
   = -- | A same column name appears twice or more in the raw 'SQL'.
     ErrStatement_DuplicateColumnName BindingName
   deriving stock (Eq, Show)
   deriving anyclass (Ex.Exception)

--------------------------------------------------------------------------------

data ErrRows
   = -- | Fewer rows than requested were available.
     ErrRows_TooFew
   | -- | More rows than requested were available.
     ErrRows_TooMany
   deriving stock (Eq, Show)
   deriving anyclass (Ex.Exception)

-- | __Fold__ the output rows from a 'Statement' in a way that allows
-- interleaving 'IO' actions.
--
-- * This is simpler alternative to 'streamIO' for when all you need to do
-- is fold.
--
-- * If you don't need to interleave 'IO' actions, then consider
-- using 'Sq.fold'.

-- Note: This could be defined in terms of 'streamIO', but this implementation
-- is faster because we avoid per-row resource management.
foldIO
   :: forall o z i t s m
    . (MonadIO m, Ex.MonadMask m, SubMode t s)
   => F.FoldM m o z
   -> A.Acquire (Transaction t)
   -- ^ How to acquire the 'Transaction' once the @m@ is executed,
   -- and how to release it when it's not needed anymore.
   --
   -- If you want this 'Statement' to be the only one in the 'Transaction',
   -- then use one of 'Sq.readTransaction', 'Sq.commitTransaction' or
   -- 'Sq.rollbackTransaction'.
   --
   -- Otherwise, if you already obtained a 'Transaction' by other means, then
   -- simply use 'pure' to wrap a 'Transaction' in 'A.Acquire'.
   -> Statement s i o
   -> i
   -> m z
foldIO (F.FoldM fstep finit fext) atx st i = do
   !bs <- hushThrow $ bindStatement st i
   !acc0 <- finit
   R.withAcquire (atx >>= rowPopper bs) \pop ->
      flip fix acc0 \k !acc ->
         liftIO pop >>= maybe (fext acc) (fstep acc >=> k)

-- | __Stream__ the output rows from a 'Statement' in a way that allows
-- interleaving 'IO' actions.
--
-- * An exclusive lock will be held on the 'Transaction' while the 'Z.Stream'
-- is producing rows.
--
-- * The 'Transaction' lock is released automatically if the 'Z.Stream' is
-- consumed until exhaustion.
--
-- * If you won't consume the 'Z.Stream' until exhaustion, then be sure to exit
-- @m@ by means of 'R.runResourceT' or similar as soon as possible in order to
-- release the 'Transaction' lock.
streamIO
   :: forall o i t s m
    . (R.MonadResource m, SubMode t s)
   => A.Acquire (Transaction t)
   -- ^ How to acquire the 'Transaction' once the 'Z.Stream' starts
   -- being consumed, and how to release it when it's not needed anymore.
   --
   -- If you want this 'Statement' to be the only one in the 'Transaction',
   -- then use one of 'Sq.readTransaction', 'Sq.commitTransaction or
   -- 'Sq.rollbackTransaction'.
   --
   -- Otherwise, if you already obtained a 'Transaction' by other means, then
   -- simply use 'pure' to wrap a 'Transaction' in 'A.Acquire'.
   -> Statement s i o
   -> i
   -> Z.Stream (Z.Of o) m ()
   -- ^ A 'Z.Stream' from the @streaming@ library.
   --
   -- We use the @streaming@ library because it is fast and doesn't
   -- add any transitive dependencies to this project.
streamIO atx st i = do
   bs <- liftIO $ hushThrow $ bindStatement st i
   (k, typop) <- lift $ A.allocateAcquire do
      pop <- rowPopper bs =<< atx
      R.mkAcquire1 (newTMVarIO (Just pop)) \tmv -> do
         atomically $ tryTakeTMVar tmv >> putTMVar tmv Nothing
   Z.untilLeft $ liftIO $ Ex.mask \restore ->
      Ex.bracket
         ( atomically do
            takeTMVar typop >>= \case
               Just pop -> pure pop
               Nothing -> Ex.throwM $ resourceVanishedWithCallStack "streamIO"
         )
         (atomically . tryPutTMVar typop . Just)
         ( restore >=> \case
            Just o -> pure $ Right o
            Nothing -> Left <$> R.releaseType k A.ReleaseEarly
         )

-- | Acquires an 'IO' action that will yield the next output row each time it
-- is called, if any. Among other 'IO' exceptions, 'ErrOutput' may be thrown.
rowPopper
   :: (SubMode t s)
   => BoundStatement s o
   -> Transaction t
   -> A.Acquire (IO (Maybe o))
rowPopper !bs Transaction{conn, di = di0} = do
   -- TODO: Could we safely prepare and bind a raw statement before
   -- lockConnection? That would be more efficient.
   xconn <- lockConnection conn
   qId <- newQueryId
   let di1 = Di.attr "query" qId di0
   ps <- acquirePreparedStatement di1 bs.sql xconn
   let di2 = Di.attr "statement" ps.id di1
       !kvs = Map.toAscList $ rawBoundInput bs.input
   R.mkAcquire1 (S.bindNamed ps.handle kvs) \_ -> S.clearBindings ps.handle
   Di.debug di2 $ "Bound " <> show kvs
   pure do
      S.step ps.handle >>= \case
         S.Row -> fmap Just do
            hushThrow =<< flip runOutput bs.output \n ->
               traverse (S.column ps.handle) (Map.lookup n ps.columns)
         S.Done -> pure Nothing

--------------------------------------------------------------------------------

-- | See 'savepoint', 'savepointRollback' and 'savepointRelease'.
--
-- * __WARNING__ safely dealing with 'Savepoint's can be tricky. Consider using
-- 'Ex.catch' on 'Sq.Transactional', which is implemented using 'Savepoint' and
-- does the right thing.
data Savepoint = Savepoint
   { id :: SavepointId
   , rollback :: IO ()
   , release :: IO ()
   }

instance NFData Savepoint where
   rnf (Savepoint !_ !_ !_) = ()

instance Show Savepoint where
   showsPrec _ x = showString "Savepoint{id = " . shows x.id . showChar '}'

-- | Obtain savepoint to which one can later 'savepointRollback' or
-- 'savepointRelease'.
savepoint :: (MonadIO m) => Transaction Write -> m Savepoint
savepoint Transaction{conn} = liftIO do
   spId <- newSavepointId
   let run' raw = R.withAcquire (lockConnection conn) \xc ->
         run xc $ flip S.exec raw
   run' $ "SAVEPOINT s" <> show' spId
   pure $
      Savepoint
         { id = spId
         , rollback = run' $ "ROLLBACK TO s" <> show' spId
         , release = run' $ "RELEASE s" <> show' spId
         }

-- | Disregard all the changes that happened to the 'Transaction'
-- related to this 'Savepoint' since the time it was obtained
-- through 'savepoint'.
--
-- * Trying to 'savepointRollback' a 'Savepoint' that isn't reachable anymore
-- throws an exception.
--
-- * A 'Savepoint' stops being reachable when the relevant 'Transaction' ends,
-- or when a 'savepointRollback' to an earlier 'Savepoint' on the same
-- 'Transaction' is performed, or when it or a later 'Savepoint' is
-- explicitely released through 'savepointRelease'.
savepointRollback :: (MonadIO m) => Savepoint -> m ()
savepointRollback s = liftIO s.rollback

-- | Release a 'Savepoint' so that it, together with any previous 'Savepoint's
-- on the same 'Transaction', become unreachable to future uses of
-- 'savepointRollback' or 'savepointRelease'.
--
-- * Trying to 'savepointRelease' a 'Savepoint' that isn't reachable anymore
-- throws an exception.
--
-- * A 'Savepoint' stops being reachable when the relevant 'Transaction' ends,
-- or when a 'savepointRollback' to an earlier 'Savepoint' on the same
-- 'Transaction' is performed, or when it or a later 'Savepoint' is
-- explicitely released through 'savepointRelease'.
savepointRelease :: (MonadIO m) => Savepoint -> m ()
savepointRelease s = liftIO s.release

--------------------------------------------------------------------------------

newtype SavepointId = SavepointId Word64
   deriving newtype (Eq, Ord, Show, NFData, Di.ToValue)

newSavepointId :: (MonadIO m) => m SavepointId
newSavepointId = SavepointId <$> newUnique

newtype StatementId = StatementId Word64
   deriving newtype (Eq, Ord, Show, NFData, Di.ToValue)

newStatementId :: (MonadIO m) => m StatementId
newStatementId = StatementId <$> newUnique

newtype TransactionId = TransactionId Word64
   deriving newtype (Eq, Ord, Show, NFData, Di.ToValue)

newTransactionId :: (MonadIO m) => m TransactionId
newTransactionId = TransactionId <$> newUnique

newtype ConnectionId = ConnectionId Word64
   deriving newtype (Eq, Ord, Show, NFData, Di.ToValue)

newConnectionId :: (MonadIO m) => m ConnectionId
newConnectionId = ConnectionId <$> newUnique

newtype QueryId = QueryId Word64
   deriving newtype (Eq, Ord, Show, NFData, Di.ToValue)

newQueryId :: (MonadIO m) => m QueryId
newQueryId = QueryId <$> newUnique
