{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoFieldSelectors #-}

module Sq
   ( -- * Value encoder
    Encoder
   , runEncoder
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
   , runDecoder
   , refineDecoder
   , refineDecoderString
   , DefaultDecoder (..)
   , decodeMaybe
   , decodeEither
   , decodeSizedIntegral
   , decodeBinary
   , decodeRead

    -- * Mode
   , Mode (..)
   , TransactionMode (..)

    -- * Statement
   , Statement
   , readStatement
   , writeStatement
   , bindStatement

    -- * Statement input
   , Input
   , encode
   , encodeWith
   , pushInput
   , absurd

    -- * Statement output
   , Output
   , decode
   , decodeWith

    -- * Raw statements
   , SQL
   , sql

    -- * Names
   , Name
   , name

    -- * Connection
   , Connection
   , ConnectionId (..)

    -- * Transaction
   , Transaction
   , TransactionId (..)

    -- * Savepoint
   , Savepoint
   , savepoint
   , rollbackToSavepoint
   , rollbacking

    -- * Rows
   , row
   , rowMaybe
   , rowsZero
   , rowsNonEmpty
   , rowsList
   , rowsStream

    -- * Connection settings
   , Settings (..)
   , defaultSettings

    -- * Pool
   , Pool
   , writePool
   , readPool
   , tempPool
   , read
   , commit
   , rollback

    -- * Resource management
    -- $resourceManagement
   , new
   , new'
   , with
   , uith

    -- * Null
   , Null (..)

    -- * Errors
   , ErrEncoder (..)
   , ErrInput (..)
   , ErrDecoder (..)
   , ErrOutput (..)
   , ErrStatement (..)
   , ErrRows (..)

    -- * Re-exports
   , S.SQLData (..)
   , S.SQLVFS (..)
   )
where

import Control.Exception.Safe qualified as Ex
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource qualified as R
import Control.Monad.Trans.Resource.Extra qualified as R
import Data.Acquire qualified as A
import Data.Function
import Database.SQLite3 qualified as S
import System.FilePath
import Prelude hiding (Read, read)

import Sq.Connection
import Sq.Decoders
import Sq.Encoders
import Sq.Input
import Sq.Mode
import Sq.Names
import Sq.Null
import Sq.Output
import Sq.Pool
import Sq.Statement
import Sq.Support

--------------------------------------------------------------------------------

-- | 'A.Acquire' through 'R.MonadResource'.
new :: (R.MonadResource m) => A.Acquire a -> m a
new = fmap snd . new'

-- | 'A.Acquire' through 'R.MonadResource', with 'R.ReleaseKey'.
new' :: (R.MonadResource m) => A.Acquire a -> m (R.ReleaseKey, a)
new' = A.allocateAcquire

-- | 'A.Acquire' through 'Ex.MonadMask'.
with :: (Ex.MonadMask m, MonadIO m) => A.Acquire a -> (a -> m b) -> m b
with = R.withAcquire

-- | 'A.Acquire' through 'R.MonadUnliftIO'.
uith :: (R.MonadUnliftIO m) => A.Acquire a -> (a -> m b) -> m b
uith = A.with

--------------------------------------------------------------------------------

-- | Acquire a 'Pool' temporarily persisted in the file-system.
-- It will be deleted once released. This can be useful for testing.
tempPool :: A.Acquire (Pool Write)
tempPool = do
   d <- acquireTmpDir
   pool $ defaultSettings (d </> "db.sqlite")

writePool :: Settings -> A.Acquire (Pool Write)
writePool = pool
{-# INLINE writePool #-}

readPool :: Settings -> A.Acquire (Pool Read)
readPool = pool
{-# INLINE readPool #-}

--------------------------------------------------------------------------------

readStatement :: Input i -> Output o -> SQL -> Statement Read i o
readStatement = statement
{-# INLINE readStatement #-}

writeStatement :: Input i -> Output o -> SQL -> Statement Write i o
writeStatement = statement
{-# INLINE writeStatement #-}
