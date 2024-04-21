{-# LANGUAGE StrictData #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoFieldSelectors #-}

module Sq.Connection
   ( Connection
   , connection
   , Transaction
   , Settings (..)
   , settings
   , readTransaction'
   , writeTransaction'
   , rowsOne
   , rowsMaybe
   , rowsZero
   , rowsList
   , rowsNonEmpty
   , rowsFold
   , rowsFoldM
   , rowsStream
   , ConnectionId (..)
   , TransactionId (..)
   , SavepointId (..)
   , Savepoint
   , savepoint
   , rollbackTo
   , ErrRows (..)
   , ErrStatement (..)
   ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Async qualified as Async
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
import Data.List.NonEmpty qualified as NEL
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
import Foreign.Ptr (FunPtr, Ptr, freeHaskellFunPtr)
import Foreign.Storable
import GHC.IO (evaluate, unsafeUnmask)
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
data Connection (mode :: Mode) = Connection
   { _id :: ConnectionId
   , timeout :: Word32
   -- ^ Same @timeout@ as in 'Settings'
   , di :: Di.Df1
   , xconn :: TMVar (Maybe (ExclusiveConnection mode))
   -- ^ 'Nothing' if the connection has vanished.
   }

instance HasField "id" (Connection mode) ConnectionId where
   getField = (._id)

instance NFData (Connection mode) where
   rnf (Connection !_ !_ !_ !_) = ()

instance Show (Connection mode) where
   showsPrec _ c = showString "Connection{id = " . shows c.id . showChar '}'

connection :: SMode mode -> Di.Df1 -> Settings -> A.Acquire (Connection mode)
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
   , run :: forall x. (S.Database -> IO x) -> IO x
   , statements :: IORef (Map SQL PreparedStatement)
   }

instance Show (ExclusiveConnection m) where
   showsPrec _ x =
      showString "ExclusiveConnection{id = " . shows x.id . showChar '}'

run :: (MonadIO m) => ExclusiveConnection mode -> (S.Database -> IO x) -> m x
run ExclusiveConnection{run = r} k = liftIO $ r k

--------------------------------------------------------------------------------

lockConnection :: Connection mode -> A.Acquire (ExclusiveConnection mode)
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
      (atomically . fmap (const ()) . tryPutTMVar c.xconn . Just)

data DatabaseMessage
   = forall x.
      DatabaseMessage
      (S.Database -> IO x)
      (Either Ex.SomeException x -> IO ())

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
   -> A.Acquire (Di.Df1, ExclusiveConnection mode)
exclusiveConnection smode di0 cs = do
   cId :: ConnectionId <- newConnectionId
   let di1 = Di.attr "id" cId $ Di.push "connection" di0
   dms :: MVar DatabaseMessage <-
      R.mkAcquire1 newEmptyMVar (fmap (const ()) . tryTakeMVar)
   abackground :: Async.Async () <-
      R.mkAcquire1
         (Async.async (background di1 (takeMVar dms)))
         Async.uninterruptibleCancel
   liftIO $ Async.link abackground
   statements :: IORef (Map SQL PreparedStatement) <-
      R.mkAcquire1 (newIORef mempty) \r ->
         atomicModifyIORef' r (mempty,) >>= traverse_ \ps ->
            Ex.tryAny (S.finalize ps.handle)
   pure
      ( di1
      , ExclusiveConnection
         { statements
         , id = cId
         , run = \ !act -> do
            mv <- newEmptyMVar
            putMVar dms $! DatabaseMessage act $ putMVar mv
            takeMVar mv >>= either Ex.throwM pure
         }
      )
  where
   background :: forall x. Di.Df1 -> IO DatabaseMessage -> IO x
   background di1 next = R.runResourceT do
      (_, db) <- do
         R.allocate
            ( do
               let di2 = Di.push "connect" di1
               db <- warningOnException di2 do
                  S.open2 (T.pack cs.file) (modeFlags (fromSMode smode)) cs.vfs
               Di.debug_ di2 "OK"
               pure db
            )
            ( \db -> do
               let di2 = Di.push "disconnect" di1
               warningOnException di1 do
                  Ex.finally
                     (Ex.uninterruptibleMask_ (S.interrupt db))
                     (S.close db)
               Di.debug_ di2 "OK"
            )
      warningOnException (Di.push "set-busy-handler" di1) do
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

-- | A database transaction handle.
--
-- @mode@ indicates whether 'Read'-only or read-'Write' 'Statement's are
-- supported.
--
-- Obtain with 'Sq.read', 'Sq.commit' or 'Sq.rollback'.
--
-- Prefer to use a 'Read'-only 'Transaction' if you are solely performing
-- 'Read'-only 'Statement's. It will be more efficient in concurrent settings.
--
-- If you have access to a 'Transaction' within its intended scope, then you
-- can assume that a database transaction has started, and will eventually be
-- automatically commited or rolled back as requested.
--
-- It's safe and efficient to use a 'Transaction' concurrently as is.
-- Concurrency is handled internally.

-- While the 'Transaction' is active, an exclusive lock is held on the
-- underlying 'Connection'.
data Transaction (mode :: Mode) = forall cmode.
    (SubMode cmode mode) =>
   Transaction
   { _id :: TransactionId
   , di :: Di.Df1
   , conn :: Connection cmode
   , commit :: Bool
   }

instance Show (Transaction mode) where
   showsPrec _ t =
      showString "Transaction{id = "
         . shows t.id
         . showString ", commit = "
         . shows t.commit
         . showChar '}'

instance NFData (Transaction mode) where
   rnf (Transaction !_ !_ !_ !_) = ()

instance HasField "id" (Transaction mode) TransactionId where
   getField = (._id)

readTransaction'
   :: (SubMode mode Read) => Connection mode -> A.Acquire (Transaction Read)
readTransaction' c = do
   xc <- lockConnection c
   tId <- newTransactionId
   let di1 = Di.attr "mode" Read $ Di.attr "id" tId $ Di.push "transaction" c.di
   R.mkAcquireType1
      ( do
         let di2 = Di.push "begin" di1
         warningOnException di2 $ run xc (flip S.exec "BEGIN DEFERRED")
         Di.debug_ di2 "OK"
      )
      ( \_ rt -> do
         let di2 = Di.push "rollback" di1
         for_ (releaseTypeException rt) \e ->
            Di.notice di2 $ "Will rollback due to: " <> show e
         warningOnException di2 $ run xc (flip S.exec "ROLLBACK")
         Di.debug_ di2 "OK"
      )
   xconn <- R.mkAcquire1 (newTMVarIO (Just xc)) \t ->
      atomically $ tryTakeTMVar t >> putTMVar t Nothing
   pure $ Transaction{_id = tId, di = di1, conn = c{xconn}, commit = False}

writeTransaction'
   :: Bool
   -- ^ Whether to finally @COMMIT@ the transaction.
   -- Otherwise, it will @ROLLBACK@.
   -> Connection Write
   -> A.Acquire (Transaction Write)
writeTransaction' commit c = do
   xc <- lockConnection c
   tId <- newTransactionId
   let di1 =
         Di.attr "commit" commit $
            Di.attr "mode" Write $
               Di.attr "id" tId $
                  Di.push "transaction" c.di
       rollback (ye :: Maybe Ex.SomeException) = do
         let di2 = Di.push "rollback" di1
         for_ ye \e -> Di.notice di2 $ "Will rollback due to: " <> show e
         warningOnException di2 $ run xc (flip S.exec "ROLLBACK")
         Di.debug_ di2 "OK"
   R.mkAcquireType1
      ( do
         let di2 = Di.push "begin" di1
         warningOnException di2 $ run xc (flip S.exec "BEGIN IMMEDIATE")
         Di.debug_ di2 "OK"
      )
      ( \_ rt -> case releaseTypeException rt of
         Nothing
            | commit -> do
               let di2 = Di.push "commit" di1
               warningOnException di2 $ run xc (flip S.exec "COMMIT")
               Di.debug_ di2 "OK"
            | otherwise -> rollback Nothing
         Just e -> rollback (Just e)
      )
   xconn <- R.mkAcquire1 (newTMVarIO (Just xc)) \t ->
      atomically $ tryTakeTMVar t >> putTMVar t Nothing
   pure $ Transaction{_id = tId, di = di1, conn = c{xconn}, commit}

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
   -> ExclusiveConnection mode
   -> A.Acquire PreparedStatement
acquirePreparedStatement di0 raw xconn = do
   let di1 = Di.push "statement" di0
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
                     let di2 = Di.attr "id" ps.id di1
                     Di.debug_ di2 "Reprepared"
                     columns <- getStatementColumnIndexes ps.handle
                     Di.debug di2 $ "Columns: " <> show (Map.toAscList columns)
                     pure ps{reprepares, columns}
            Nothing -> do
               stId <- newStatementId
               let di2 = Di.attr "id" stId di1
               Di.debug di2 $ "Preparing " <> show raw
               handle <- run xconn $ flip S.prepare raw.text
               reprepares <- getStatementStatusReprepare handle
               columns <- getStatementColumnIndexes handle
               Di.debug di2 $ "Columns: " <> show (Map.toAscList columns)
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
   foldM
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

-- | Like 'rowsList'. Throws 'ErrRows_TooFew' if no rows.
rowsOne
   :: (MonadIO m, SubMode t s)
   => A.Acquire (Transaction t)
   -- ^ How to obtain the 'Transaction' once @m@ is evaluated.
   --
   -- Use 'pure' to wrap a 'Transaction' in 'A.Acquire' if you
   -- already obatained a 'Transaction' by other means.
   -> Statement s i o
   -> i
   -> m o
rowsOne atx st i = liftIO do
   rowsMaybe atx st i >>= \case
      Just o -> pure o
      Nothing -> Ex.throwM ErrRows_TooFew

-- | Like 'rowsList'. Throws 'ErrRows_TooMany' if more than 0 rowsOne.
rowsZero
   :: (MonadIO m, SubMode t s)
   => A.Acquire (Transaction t)
   -- ^ How to obtain the 'Transaction' once @m@ is evaluated.
   --
   -- Use 'pure' to wrap a 'Transaction' in 'A.Acquire' if you
   -- already obatained a 'Transaction' by other means.
   -> Statement s i o
   -> i
   -> m ()
rowsZero atx st i = liftIO $ rowsFoldM f atx st i
  where
   f :: forall x. F.FoldM IO x ()
   f = F.FoldM (\_ _ -> Ex.throwM ErrRows_TooMany) (pure ()) pure

-- | Like 'rowsList'. Throws 'ErrRows_TooMany' if more than 1 rowsOne.
rowsMaybe
   :: (MonadIO m, SubMode t s)
   => A.Acquire (Transaction t)
   -- ^ How to obtain the 'Transaction' once @m@ is evaluated.
   --
   -- Use 'pure' to wrap a 'Transaction' in 'A.Acquire' if you
   -- already obatained a 'Transaction' by other means.
   -> Statement s i o
   -> i
   -> m (Maybe o)
rowsMaybe atx st i = liftIO $ rowsFoldM f atx st i
  where
   f :: forall x. F.FoldM IO x (Maybe x)
   f =
      F.FoldM
         (maybe (pure . Just) \_ _ -> Ex.throwM ErrRows_TooMany)
         (pure Nothing)
         pure

-- | Like 'rowsList'. Throws 'ErrRows_TooFew' if no rows.
rowsNonEmpty
   :: (MonadIO m, SubMode t s)
   => A.Acquire (Transaction t)
   -- ^ How to obtain the 'Transaction' once @m@ is evaluated.
   --
   -- Use 'pure' to wrap a 'Transaction' in 'A.Acquire' if you
   -- already obatained a 'Transaction' by other means.
   -> Statement s i o
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
   :: (MonadIO m, SubMode t s)
   => A.Acquire (Transaction t)
   -- ^ How to obtain the 'Transaction' once @m@ is evaluated.
   --
   -- Use 'pure' to wrap a 'Transaction' in 'A.Acquire' if you
   -- already obatained a 'Transaction' by other means.
   -> Statement s i o
   -> i
   -> m (Int64, [o])
rowsList atx st i = liftIO do
   rowsFoldM (F.generalize ((,) <$> F.genericLength <*> F.list)) atx st i

rowsFold
   :: (MonadIO m, SubMode t s)
   => F.Fold o z
   -> A.Acquire (Transaction t)
   -- ^ How to obtain the 'Transaction' once @m@ is evaluated.
   --
   -- Use 'pure' to wrap a 'Transaction' in 'A.Acquire' if you
   -- already obatained a 'Transaction' by other means.
   -> Statement s i o
   -> i
   -> m z
rowsFold f atx st i = liftIO do
   rowsFoldM (F.generalize f) atx st i

-- Note: All of the rowsXxx functions can be implemented in terms of
-- rowsStream, but it is faster to do it through this rowsFoldM
-- because we can avoid per-row resource management.
rowsFoldM
   :: (MonadIO m, Ex.MonadMask m, SubMode t s)
   => F.FoldM m o z
   -> A.Acquire (Transaction t)
   -- ^ How to obtain the 'Transaction' once @m@ is evaluated.
   --
   -- Use 'pure' to wrap a 'Transaction' in 'A.Acquire' if you
   -- already obatained a 'Transaction' by other means.
   -> Statement s i o
   -> i
   -> m z
rowsFoldM (F.FoldM !fstep !finit !fext) !atx !st !i = do
   acc0 <- finit
   R.withAcquire (transactionBoundPreparedStatement atx st i) \ps ->
      flip fix acc0 \k !acc ->
         liftIO (S.step ps.handle) >>= \case
            S.Row -> do
               eo <- liftIO $ runStatementOutput st \n ->
                  traverse (S.column ps.handle) (Map.lookup n ps.columns)
               either Ex.throwM (fstep acc >=> k) eo
            S.Done -> fext acc

transactionBoundPreparedStatement
   :: (SubMode t s)
   => A.Acquire (Transaction t)
   -> Statement s i o
   -> i
   -> A.Acquire PreparedStatement
transactionBoundPreparedStatement !atx !st !i = do
   -- TODO: Could we safely prepare and bind a statement before acquiring
   -- the transaction/connection lock? That would be more efficient.
   binput <- liftIO $ either Ex.throwM evaluate $ runStatementInput st i
   Transaction{conn, di = di0} <- atx
   xconn <- lockConnection conn
   qId <- newQueryId
   let di1 = Di.attr "id" qId $ Di.push "query" di0
   ps <- acquirePreparedStatement di1 st.sql xconn
   let di2 = Di.attr "id" ps.id $ Di.push "statement" di1
       kvs = runBoundInput binput
   R.mkAcquire1 (S.bindNamed ps.handle kvs) \_ -> S.clearBindings ps.handle
   Di.debug di2 $ "Bound " <> show kvs
   pure ps

-- | Stream of output rows.
--
-- An exclusive lock will be held on the 'Transaction' while the 'Z.Stream' is
-- producing rows. This may not be a problem if we are streaming from a
-- \''Read' 'Transaction' obtained from a 'Sq.Pool', because more
-- 'Transaction's can be readily be obtained from said 'Sq.Pool' and used
-- concurrently for other purposes. But if we are streaming from a \''Write'
-- 'Transaction', then all other concurrent attempts to perform a \''Write'
-- 'Transaction' will be delayed. With that in mind, consider this:
--
-- * The 'Transaction' lock is released automatically if the 'Z.Stream' is
-- consumed until exhaustion.
--
-- * Otherwise, if you won't consume the 'Z.Stream' until exhaustion, then be
-- sure to exit @m@ by means of 'R.runResourceT' or similar as soon as possible
-- in order to release the 'Transaction' lock.
rowsStream
   :: (R.MonadResource m, SubMode t s)
   => A.Acquire (Transaction t)
   -- ^ How to obtain the 'Transaction' once the 'Z.Stream' starts
   -- being consumed.
   --
   -- Use 'pure' to wrap a 'Transaction' in 'A.Acquire' if you
   -- already obatained a 'Transaction' by other means.
   -> Statement s i o
   -> i
   -> Z.Stream (Z.Of o) m ()
   -- ^ A 'Z.Stream' from the @streaming@ library.
   --
   -- We use the @streaming@ library because it is fast and doesn't
   -- add any transitive dependencies to this project.
rowsStream atx !st i = do
   (k, typs) <- lift $ A.allocateAcquire do
      ps <- transactionBoundPreparedStatement atx st i
      typs <- R.mkAcquire1 (newTMVarIO (Just ps)) \typs -> do
         atomically $ tryTakeTMVar typs >> putTMVar typs Nothing
      pure (typs :: TMVar (Maybe PreparedStatement))
   Z.untilLeft $ liftIO $ Ex.mask \restore ->
      Ex.bracket
         ( atomically do
            takeTMVar typs >>= \case
               Just ps -> pure ps
               Nothing ->
                  -- `m` was run before the `Z.Stream` was fully consumed.
                  -- This is normal, we just want to throw an useful exception.
                  Ex.throwM $ resourceVanishedWithCallStack "Rows"
         )
         (atomically . fmap (const ()) . tryPutTMVar typs . Just)
         \ps ->
            restore (S.step ps.handle) >>= \case
               S.Row ->
                  either Ex.throwM (pure . Right) =<< restore do
                     runStatementOutput st \n ->
                        traverse (S.column ps.handle) (Map.lookup n ps.columns)
               S.Done ->
                  -- The `Z.Stream` finished before `m` was run, so we release
                  -- the transaction early.
                  Left <$> R.releaseType k A.ReleaseEarly

--------------------------------------------------------------------------------

-- | See 'savepoint'.
data Savepoint = Savepoint
   { id :: SavepointId
   , rollback :: IO ()
   }

instance NFData Savepoint where
   rnf (Savepoint !_ !_) = ()

instance Show Savepoint where
   showsPrec _ x = showString "Savepoint{id = " . shows x.id . showChar '}'

-- | Obtain savepoint to which one can later 'rollbackTo'.
savepoint :: (MonadIO m) => Transaction Write -> m Savepoint
savepoint Transaction{conn} = liftIO do
   spId <- newSavepointId
   R.withAcquire (lockConnection conn) \xc ->
      run xc $ flip S.exec ("SAVEPOINT s" <> show' spId)
   pure $
      Savepoint
         { id = spId
         , rollback =
            R.withAcquire (lockConnection conn) \xc ->
               run xc $ flip S.exec ("ROLLBACK TO s" <> show' spId)
         }

-- | Disregard all the changes that happened to the 'Transaction'
-- related to this 'Savepoint' since the time it was obtained
-- through 'savepoint'.
--
-- Trying to 'rollbackTo' a 'Savepoint' that isn't reachable anymore
-- throws an exception.  A 'Savepoint' stops being reachable when the
-- relevant 'Transaction' ends, or when of a 'rollbackTo' an earlier
-- 'Savepoint' on the same 'Transaction' is performed.
rollbackTo :: (MonadIO m) => Savepoint -> m ()
rollbackTo s = liftIO s.rollback

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
