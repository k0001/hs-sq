{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoFieldSelectors #-}

module Sq
   ( -- * Statement
    Statement
   , readStatement
   , writeStatement
   , bindStatement

    -- ** SQL
   , SQL
   , sql

    -- ** Input
   , Input
   , encode
   , input

    -- *** Encode
   , Encode (..)
   , encodeRefine
   , EncodeDefault (..)
   , encodeMaybe
   , encodeEither
   , encodeSizedIntegral
   , encodeBinary
   , encodeShow

    -- ** Output
   , Output
   , decode
   , output

    -- *** Decode
   , Decode (..)
   , decodeRefine
   , DecodeDefault (..)
   , decodeMaybe
   , decodeEither
   , decodeSizedIntegral
   , decodeBinary
   , decodeRead

    -- ** Name
   , Name
   , name

    -- * Rows
   , rowsOne
   , rowsMaybe
   , rowsZero
   , rowsNonEmpty
   , rowsList
   , rowsFold
   , rowsFoldM
   , rowsStream

    -- * Transaction
   , Transaction
   , read
   , commit
   , rollback

    -- ** Savepoint
   , Savepoint
   , savepoint
   , rollbackTo

    -- * Pool
   , Pool
   , poolRead
   , poolWrite
   , poolTemp

    -- ** Settings
   , Settings (..)
   , settings


    -- * Resource management
    -- $resourceManagement
   , new
   , with
   , uith

    -- * Errors
   , ErrEncode (..)
   , ErrInput (..)
   , ErrDecode (..)
   , ErrOutput (..)
   , ErrStatement (..)
   , ErrRows (..)

    -- * Miscellaneuos
   , BindingName
   , Mode (..)
   , Null (..)
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
-- $resourceManagement
--
-- "Sq" relies heavily on 'A.Acquire' for safe resource management in light of
-- concurrency and dependencies between resources. But as a user of "Sq", it is
-- unlikely that you will want to deal with 'A.Acquire' directly.  We don't
-- force any particulary resource management tool on you. Instead, we export
-- three tools for you to adapt 'A.Acquire' to your needs:
--
-- * 'with' for integrating with 'Ex.MonadMask' from
-- the @exceptions@ library.
--
-- * 'new' for integrating with 'R.MonadResource' from
-- the @resourcet@ library.
--
-- * 'uith' for integrating with 'R.MonadUnliftIO' from
-- the @unliftio@ library.
--
-- If you don't have any opinion about which of 'with', 'new' and 'with' to
-- use, just use 'with'. Here is an example of how to use 'with' to acquire
-- a new 'Pool' through 'poolWrite':
--
-- @
-- 'with' ('poolWrite' ('settings' \"\/my\/db.sqlite\"))
--     \\(__pool__ :: 'Pool' \''Write') ->
--            /... Here use __pool__ as necessary./
--            /... The resources associated with it will be/
--            /... automatically released after leaving this scope./
-- @

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
   pool SWrite di1 $ settings (d </> "db.sqlite")

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
