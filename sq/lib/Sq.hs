{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoFieldSelectors #-}

module Sq
   ( -- * Value encode
    Encode
   , runEncode
   , refineEncode
   , refineEncodeString
   , DefaultEncode (..)
   , encodeMaybe
   , encodeEither
   , encodeSizedIntegral
   , encodeBinary
   , encodeShow

    -- * Value decodes
   , Decode
   , runDecode
   , refineDecode
   , refineDecodeString
   , DefaultDecode (..)
   , decodeMaybe
   , decodeEither
   , decodeSizedIntegral
   , decodeBinary
   , decodeRead

    -- * Mode
   , Mode (..)

    -- * Statement
   , Statement
   , readStatement
   , writeStatement
   , bindStatement

    -- * Statement input
   , Input
   , encode
   , input

    -- * Statement output
   , Output
   , decode
   , output

    -- * Raw statements
   , SQL
   , sql

    -- * Names
   , Name
   , name
   , BindingName

    -- * Transaction
   , Transaction
   , read
   , commit
   , rollback

    -- * Savepoint
   , Savepoint
   , savepoint
   , rollbackTo

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
   , poolRead
   , poolWrite
   , poolTemp

    -- * Resource management
    -- $resourceManagement
   , new
   , with
   , uith

    -- * Null
   , Null (..)

    -- * Errors
   , ErrEncode (..)
   , ErrInput (..)
   , ErrDecode (..)
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
import Di.Df1 qualified as Di
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
--
-- @
-- 'new' = 'fmap' 'snd' . "Data.Acquire".'A.allocateAcquire'
-- @
new :: (R.MonadResource m) => A.Acquire a -> m a
new = fmap snd . A.allocateAcquire

-- | 'A.Acquire' through 'Ex.MonadMask'.
--
-- @
-- 'with' = "Control.Monad.Trans.Resource.Extra".'R.withAcquire'.
-- @
with :: (Ex.MonadMask m, MonadIO m) => A.Acquire a -> (a -> m b) -> m b
with = R.withAcquire

-- | 'A.Acquire' through 'R.MonadUnliftIO'.
--
-- @
-- 'uith' = "Data.Acquire".'A.with'
-- @
uith :: (R.MonadUnliftIO m) => A.Acquire a -> (a -> m b) -> m b
uith = A.with

--------------------------------------------------------------------------------

-- | Acquire a read-'Write' 'Pool' temporarily persisted in the file-system.
-- It will be deleted once released. This can be useful for testing.
--
-- Use "Di".'Di.new' to obtain the 'Di.Df1' parameter. Consider using
-- "Di.Core".'Di.Core.filter' to filter-out excessive logging.
poolTemp :: Di.Df1 -> A.Acquire (Pool Write)
poolTemp di0 = do
   d <- acquireTmpDir
   let di1 = Di.attr "mode" Write $ Di.push "pool" di0
   pool SWrite di1 $ defaultSettings (d </> "db.sqlite")

-- | Acquire a read-'Write' 'Pool' according to the given 'Settings'.
--
-- Use "Di".'Di.new' to obtain the 'Di.Df1' parameter. Consider using
-- "Di.Core".'Di.Core.filter' to filter-out excessive logging.
poolWrite :: Di.Df1 -> Settings -> A.Acquire (Pool Write)
poolWrite di0 s = do
   let di1 = Di.attr "mode" Write $ Di.push "pool" di0
   pool SWrite di1 s
{-# INLINE poolWrite #-}

-- | Acquire a 'Read'-only 'Pool' according to the given 'Settings'.
--
-- Use "Di".'Di.new' to obtain the 'Di.Df1' parameter. Consider using
-- "Di.Core".'Di.Core.filter' to filter-out excessive logging.
poolRead :: Di.Df1 -> Settings -> A.Acquire (Pool Read)
poolRead di0 s = do
   let di1 = Di.attr "mode" Read $ Di.push "pool" di0
   pool SRead di1 s
{-# INLINE poolRead #-}

--------------------------------------------------------------------------------

readStatement :: Input i -> Output o -> SQL -> Statement Read i o
readStatement = statement
{-# INLINE readStatement #-}

writeStatement :: Input i -> Output o -> SQL -> Statement Write i o
writeStatement = statement
{-# INLINE writeStatement #-}
