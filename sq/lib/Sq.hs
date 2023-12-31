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
   , withConnection
   , withRollbackingTransaction
   , withCommittingTransaction

    -- ** MonadUnliftIO
   , uithConnection
   , uithRollbackingTransaction
   , uithCommittingTransaction

    -- ** MonadResource
   , newConnection
   , newRollbackingTransaction
   , newCommittingTransaction

    -- ** Acquire
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

import Control.Exception.Safe qualified as Ex
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource qualified as R
import Control.Monad.Trans.Resource.Extra qualified as R
import Data.Acquire qualified as A
import Database.SQLite3 qualified as S

import Sq.Decoders
import Sq.Encoders
import Sq.Internal

--------------------------------------------------------------------------------
-- ResourceT

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
