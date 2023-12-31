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

    -- * Statement output
   , Output
   , decode

    -- * Raw statements
   , RawStatement (..)
   , rawStatement

    -- * Names
   , Name
   , name
   , unName

    -- * ConnectionString
   , ConnectionString (..)

    -- * Pool
   , Pool

    -- * Connection
   , Connection

    -- * Transaction
   , Transaction

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
   , ErrConnection (..)
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
import Database.SQLite3 qualified as S

import Sq.Decoders
import Sq.Encoders
import Sq.Internal

--------------------------------------------------------------------------------
-- ResourceT

newPool
   :: (R.MonadResource m)
   => ConnectionString
   -> [S.SQLOpenFlag]
   -> S.SQLVFS
   -> m (R.ReleaseKey, Pool)
newPool cs flags vfs = A.allocateAcquire $ acquirePool cs flags vfs

newPoolConnection :: (R.MonadResource m) => Pool -> m (R.ReleaseKey, Connection)
newPoolConnection p = A.allocateAcquire $ acquirePoolConnection p

newConnection
   :: (R.MonadResource m)
   => ConnectionString
   -> [S.SQLOpenFlag]
   -> S.SQLVFS
   -> m (R.ReleaseKey, Connection)
newConnection cs flags vfs = A.allocateAcquire $ acquireConnection cs flags vfs

-- | @BEGIN@s a database transaction which will be @ROLLBACK@ed when released.
newRollbackingTransaction
   :: (R.MonadResource m)
   => Connection
   -> m (R.ReleaseKey, Transaction)
newRollbackingTransaction conn =
   A.allocateAcquire $ acquireCommittingTransaction conn

-- | @BEGIN@s a database transaction. If released with 'A.ReleaseExceptionWith',
-- then the transaction is @ROLLBACK@ed. Otherwise, it is @COMMIT@ed.
newCommittingTransaction
   :: (R.MonadResource m)
   => Connection
   -> m (R.ReleaseKey, Transaction)
newCommittingTransaction conn =
   A.allocateAcquire $ acquireCommittingTransaction conn

--------------------------------------------------------------------------------
-- MonadMask

withPool
   :: (Ex.MonadMask m, MonadIO m)
   => ConnectionString
   -> [S.SQLOpenFlag]
   -> S.SQLVFS
   -> (Pool -> m a)
   -> m a
withPool cs flags vfs = R.withAcquire $ acquirePool cs flags vfs

withPoolConnection
   :: (Ex.MonadMask m, MonadIO m) => Pool -> (Connection -> m a) -> m a
withPoolConnection p = R.withAcquire $ acquirePoolConnection p

withConnection
   :: (Ex.MonadMask m, MonadIO m)
   => ConnectionString
   -> [S.SQLOpenFlag]
   -> S.SQLVFS
   -> (Connection -> m a)
   -> m a
withConnection cs flags vfs = R.withAcquire $ acquireConnection cs flags vfs

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

uithPool
   :: (R.MonadUnliftIO m)
   => ConnectionString
   -> [S.SQLOpenFlag]
   -> S.SQLVFS
   -> (Pool -> m a)
   -> m a
uithPool cs flags vfs = A.with $ acquirePool cs flags vfs

uithPoolConnection :: (R.MonadUnliftIO m) => Pool -> (Connection -> m a) -> m a
uithPoolConnection p = A.with $ acquirePoolConnection p

uithConnection
   :: (R.MonadUnliftIO m)
   => ConnectionString
   -> [S.SQLOpenFlag]
   -> S.SQLVFS
   -> (Connection -> m a)
   -> m a
uithConnection cs flags vfs = A.with $ acquireConnection cs flags vfs

-- | @BEGIN@s a database transaction which will be @ROLLBACK@ed when released.
uithRollbackingTransaction
   :: (R.MonadUnliftIO m)
   => Connection
   -> (Transaction -> m a)
   -> m a
uithRollbackingTransaction conn = A.with $ acquireRollbackingTransaction conn

-- | @BEGIN@s a database transaction. If released with 'A.ReleaseExceptionWith',
-- then the transaction is @ROLLBACK@ed. Otherwise, it is @COMMIT@ed.
uithCommittingTransaction
   :: (R.MonadUnliftIO m)
   => Connection
   -> (Transaction -> m a)
   -> m a
uithCommittingTransaction conn = A.with $ acquireCommittingTransaction conn

--------------------------------------------------------------------------------
-- Pool

newtype Pool = Pool (P.Pool (A.Allocated Connection))

acquirePool :: ConnectionString -> [S.SQLOpenFlag] -> S.SQLVFS -> A.Acquire Pool
acquirePool cs flags vfs = fmap Pool do
   R.acquire1
      ( \res -> do
         let A.Acquire f = acquireConnection cs flags vfs
         n <- getNumCapabilities
         P.newPool $
            P.defaultPoolConfig
               (f res)
               (\(A.Allocated _ g) -> g A.ReleaseNormal)
               10
               n
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
