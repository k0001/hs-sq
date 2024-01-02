{-# LANGUAGE QuasiQuotes #-}

module Sq
   ( -- * Value encoder
    Encoder
   , refineEncoder
   , refineEncoderString
   , DefaultEncoder (..)
   , encodeMaybe
   , encodeEither
   , encodeSizedIntegral
   , encodeBinary
   , encodeShow

    -- * Value decoders
   , Decoder
   , refineDecoder
   , refineDecoderString
   , DefaultDecoder (..)
   , decodeMaybe
   , decodeEither
   , decodeSizedIntegral
   , decodeBinary
   , decodeRead

    -- * Statement
   , Statement
   , statement

    -- * Statement input
   , Input
   , encode
   , push
   , void

    -- * Statement output
   , Output
   , decode

    -- * Raw statements
   , RawStatement (..)
   , rawStatement

    -- * Bound statement
   , BoundStatement
   , bindStatement

    -- * Names
   , Name
   , name
   , unName

    -- * Settings
   , Settings (..)
   , defaultSettingsReadOnly
   , defaultSettingsReadWrite
   , defaultLogStderr

    -- * Pool
   , Pool

    -- * Connection
   , Connection
   , ConnectionId (..)

    -- * Transaction
   , Transaction
   , TransactionId (..)

    -- * Rows
   , row
   , rowMaybe
   , rowsNonEmpty
   , rowsList
   , rowsStream

    -- * Resource management

    -- ** MonadMask
   , withPool
   , withPoolConnection
   , withConnection
   , withRollbackingTransaction
   , withCommittingTransaction

    -- ** MonadUnliftIO
   , uithPool
   , uithPoolConnection
   , uithConnection
   , uithRollbackingTransaction
   , uithCommittingTransaction

    -- ** MonadResource
   , newPool
   , newPoolConnection
   , newConnection
   , newRollbackingTransaction
   , newCommittingTransaction

    -- ** Acquire
   , acquirePool
   , acquirePoolConnection
   , acquireConnection
   , acquireRollbackingTransaction
   , acquireCommittingTransaction

    -- * Errors
   , ErrDecoder (..)
   , ErrBinding (..)
   , ErrOutput (..)
   , ErrStatement (..)
   , ErrRows (..)

    -- * Re-exports
   , S.SQLData (..)
   , S.SQLOpenFlag (..)
   , S.SQLVFS (..)
   )
where

import Control.Concurrent
import Control.Exception.Safe qualified as Ex
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource qualified as R
import Control.Monad.Trans.Resource.Extra qualified as R
import Data.Acquire qualified as A
import Data.Acquire.Internal qualified as A
import Data.Pool qualified as P
import Data.Text qualified as T
import Database.SQLite3 qualified as S
import System.IO qualified as IO

import Sq.Decoders
import Sq.Encoders
import Sq.Internal

--------------------------------------------------------------------------------
-- ResourceT

newPool :: (R.MonadResource m) => Settings -> m (R.ReleaseKey, Pool)
newPool = A.allocateAcquire . acquirePool

newPoolConnection :: (R.MonadResource m) => Pool -> m (R.ReleaseKey, Connection)
newPoolConnection = A.allocateAcquire . acquirePoolConnection

newConnection :: (R.MonadResource m) => Settings -> m (R.ReleaseKey, Connection)
newConnection = A.allocateAcquire . acquireConnection

-- | @BEGIN@s a database transaction which will be @ROLLBACK@ed when released.
newRollbackingTransaction
   :: (R.MonadResource m) => Connection -> m (R.ReleaseKey, Transaction)
newRollbackingTransaction =
   A.allocateAcquire . acquireRollbackingTransaction

-- | @BEGIN@s a database transaction. If released with 'A.ReleaseExceptionWith',
-- then the transaction is @ROLLBACK@ed. Otherwise, it is @COMMIT@ed.
newCommittingTransaction
   :: (R.MonadResource m) => Connection -> m (R.ReleaseKey, Transaction)
newCommittingTransaction =
   A.allocateAcquire . acquireCommittingTransaction

--------------------------------------------------------------------------------
-- MonadMask

withPool :: (Ex.MonadMask m, MonadIO m) => Settings -> (Pool -> m a) -> m a
withPool s = R.withAcquire (acquirePool s)

withPoolConnection
   :: (Ex.MonadMask m, MonadIO m) => Pool -> (Connection -> m a) -> m a
withPoolConnection p = R.withAcquire (acquirePoolConnection p)

withConnection
   :: (Ex.MonadMask m, MonadIO m) => Settings -> (Connection -> m a) -> m a
withConnection s = R.withAcquire (acquireConnection s)

-- | @BEGIN@s a database transaction which will be @ROLLBACK@ed when released.
withRollbackingTransaction
   :: (Ex.MonadMask m, MonadIO m)
   => Connection
   -> (Transaction -> m a)
   -> m a
withRollbackingTransaction conn =
   R.withAcquire $ acquireRollbackingTransaction conn

-- | @BEGIN@s a database transaction. If released with 'A.ReleaseExceptionWith',
-- then the transaction is @ROLLBACK@ed. Otherwise, it is @COMMIT@ed.
withCommittingTransaction
   :: (Ex.MonadMask m, MonadIO m)
   => Connection
   -> (Transaction -> m a)
   -> m a
withCommittingTransaction conn =
   R.withAcquire $ acquireCommittingTransaction conn

--------------------------------------------------------------------------------
-- MonadUnliftIO

uithPool :: (R.MonadUnliftIO m) => Settings -> (Pool -> m a) -> m a
uithPool s = A.with (acquirePool s)

uithPoolConnection :: (R.MonadUnliftIO m) => Pool -> (Connection -> m a) -> m a
uithPoolConnection p = A.with (acquirePoolConnection p)

uithConnection :: (R.MonadUnliftIO m) => Settings -> (Connection -> m a) -> m a
uithConnection s = A.with (acquireConnection s)

-- | @BEGIN@s a database transaction which will be @ROLLBACK@ed when released.
uithRollbackingTransaction
   :: (R.MonadUnliftIO m) => Connection -> (Transaction -> m a) -> m a
uithRollbackingTransaction c = A.with (acquireRollbackingTransaction c)

-- | @BEGIN@s a database transaction. If released with 'A.ReleaseExceptionWith',
-- then the transaction is @ROLLBACK@ed. Otherwise, it is @COMMIT@ed.
uithCommittingTransaction
   :: (R.MonadUnliftIO m) => Connection -> (Transaction -> m a) -> m a
uithCommittingTransaction c = A.with (acquireCommittingTransaction c)

--------------------------------------------------------------------------------
-- Pool

newtype Pool = Pool (P.Pool (A.Allocated Connection))

acquirePool :: Settings -> A.Acquire Pool
acquirePool s = fmap Pool do
   R.acquire1
      ( \res -> do
         let A.Acquire f = acquireConnection s
         n <- getNumCapabilities
         P.newPool $
            P.defaultPoolConfig
               (f res)
               (\(A.Allocated _ g) -> g A.ReleaseNormal)
               10 -- timeout seconds
               (max 10 n) -- approximate max total connections to keep open
      )
      P.destroyAllResources

acquirePoolConnection :: Pool -> A.Acquire Connection
acquirePoolConnection (Pool p) = fmap (\(A.Allocated c _, _) -> c) do
   R.mkAcquireType1
      (P.takeResource p)
      \(a@(A.Allocated _ rel), lp) t -> case t of
         A.ReleaseExceptionWith _ ->
            rel t `Ex.finally` P.destroyResource p lp a
         _ -> P.putResource lp a

--------------------------------------------------------------------------------

defaultSettingsReadOnly :: T.Text -> Settings
defaultSettingsReadOnly database =
   Settings
      { database
      , flags = [S.SQLOpenReadOnly, S.SQLOpenWAL, S.SQLOpenFullMutex]
      , vfs = S.SQLVFSDefault
      , log = \_ _ _ -> pure ()
      }

defaultSettingsReadWrite :: T.Text -> Settings
defaultSettingsReadWrite database =
   Settings
      { database
      , flags =
         [ S.SQLOpenReadWrite
         , S.SQLOpenCreate
         , S.SQLOpenWAL
         , S.SQLOpenFullMutex
         ]
      , vfs = S.SQLVFSDefault
      , log = \_ _ _ -> pure ()
      }

defaultLogStderr :: ConnectionId -> Maybe TransactionId -> String -> IO ()
defaultLogStderr c = \yt m ->
   IO.hPutStrLn IO.stderr $
      mconcat $
         mconcat
            [ ["connection=", show c, " "]
            , maybe [] (\t -> ["transaction=", show t, " "]) yt
            , [m]
            ]
