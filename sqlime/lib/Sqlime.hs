{-# LANGUAGE QuasiQuotes #-}

module Sqlime
   ( -- * Value encoder
    Encoder
   , refineEncoder
   , refineEncoderString
   , DefaultEncoder (..)
   , encodeMaybe
   , encodeEither
   , encodeSizedIntegral

    -- * Value decoders
   , Decoder
   , refineDecoder
   , refineDecoderString
   , DefaultDecoder (..)
   , decodeMaybe
   , decodeEither
   , decodeSizedIntegral

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
   , withTransaction

    -- ** MonadUnliftIO
   , uithConnection
   , uithTransaction

    -- ** MonadResource
   , newConnection
   , newTransaction

    -- ** Acquire
   , acquireConnection
   , acquireTransaction

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

import Sqlime.Decoders
import Sqlime.Encoders
import Sqlime.Internal

--------------------------------------------------------------------------------
-- ResourceT

newConnection
   :: (R.MonadResource m)
   => ConnectionString
   -> [S.SQLOpenFlag]
   -> S.SQLVFS
   -> m (R.ReleaseKey, Connection)
newConnection cs flags vfs = A.allocateAcquire $ acquireConnection cs flags vfs

-- | @BEGIN@s a database transaction. If released with 'A.ReleaseExceptionWith',
-- then the transaction is @ROLLBACK@ed. Otherwise, it is @COMMIT@ed.
newTransaction
   :: (R.MonadResource m)
   => Connection
   -> m (R.ReleaseKey, Transaction)
newTransaction conn = A.allocateAcquire $ acquireTransaction conn

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

-- | @BEGIN@s a database transaction. If released with 'A.ReleaseExceptionWith',
-- then the transaction is @ROLLBACK@ed. Otherwise, it is @COMMIT@ed.
withTransaction
   :: (Ex.MonadMask m, MonadIO m)
   => Connection
   -> (Transaction -> m a)
   -> m a
withTransaction conn = R.withAcquire $ acquireTransaction conn

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

-- | @BEGIN@s a database transaction. If released with 'A.ReleaseExceptionWith',
-- then the transaction is @ROLLBACK@ed. Otherwise, it is @COMMIT@ed.
uithTransaction
   :: (R.MonadUnliftIO m)
   => Connection
   -> (Transaction -> m a)
   -> m a
uithTransaction conn = A.with $ acquireTransaction conn
