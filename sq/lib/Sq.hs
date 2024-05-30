{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoFieldSelectors #-}

-- | High-level SQLite client library
--
-- @
-- import qualified "Sq"
-- @
--
-- Things currently supported:
--
-- * Type-safe __encoding__ of SQL query parameters and columns ('Encode',
-- 'Input').
--
-- * Type-safe __decoding__ of SQL output rows and columns ('Decode', 'Output').
--
-- * Type-safe __concurrent connections__ with read and write database access
-- ('Pool').
--
-- * Type-safe __'Control.Concurrent.STM'-like transactional__ interactions
-- with the database, including 'Control.Concurrent.STM.retry'-like,
-- 'Control.Concurrent.STM.TVar'-like, and
-- 'Control.Concurrent.STM.catchSTM'-like tools ('Transactional', 'retry',
-- 'Ref').
--
-- * Type-safe __distinction between 'Read'-only and read-'Write'__ things.
--
-- * Type-safe __streaming and interleaving of 'IO'__ with output rows
-- ('streamIO', 'foldIO').
--
-- * Type-safe __resource management__ (via 'A.Acquire', see 'new', 'with',
-- 'uith').
--
-- * Manual __transactional migrations__ ('migrate').
--
-- * 'Savepoint's.
--
-- * A lot of logging.
--
-- Things not supported yet:
--
-- * Type-safe 'SQL'.
--
--
-- * Probably other things.
--
-- If you have questions or suggestions, just say so at
-- <https://github.com/k0001/hs-sq/issues>.
--
-- Note: This library is young and needs more testing.
module Sq
   ( -- * Statement
    Statement
   , readStatement
   , writeStatement

    -- ** SQL
   , SQL
   , sql

    -- ** Input
   , Input
   , encode
   , input
   , hinput
   , HInput
   , InputDefault (..)

    -- *** Encode
   , Encode (..)
   , encodeRefine
   , EncodeDefault (..)
   , encodeMaybe
   , encodeEither
   , encodeNS
   , encodeSizedIntegral
   , encodeAeson
   , encodeAeson'
   , encodeBinary
   , encodeBinary'
   , encodeShow

    -- ** Output
   , Output
   , decode
   , output
   , houtput
   , HOutput
   , OutputDefault (..)

    -- *** Decode
   , Decode (..)
   , decodeRefine
   , DecodeDefault (..)
   , decodeMaybe
   , decodeEither
   , decodeNS
   , decodeSizedIntegral
   , decodeAeson
   , decodeAeson'
   , decodeBinary
   , decodeBinary'
   , decodeRead

    -- ** Name
   , Name
   , name
   , BindingName (..)

    -- * Transactional
   , Transactional
   , read
   , commit
   , rollback
   , embed
   , Ref
   , retry
   , orElse

    -- ** Querying
   , one
   , maybe
   , zero
   , some
   , list
   , fold
   , foldM

    -- * Interleaving
   , streamIO
   , foldIO

    -- * Pool
   , Pool
   , readPool
   , writePool
   , tempPool
   , subPool

    -- * Settings
   , Settings (..)
   , settings

    -- * Transaction
   , Transaction
   , readTransaction
   , commitTransaction
   , rollbackTransaction

    -- * Resources
    -- $resources
   , new
   , with
   , uith

    -- * Savepoint
   , Savepoint
   , savepoint
   , savepointRollback
   , savepointRelease

    -- * Migrations
    -- $migrations
   , migrate
   , migration
   , Migration
   , MigrationId
   , MigrationsTable

    -- * Miscellaneuos
   , Retry (..)
   , Mode (..)
   , SubMode
   , Null (..)

    -- * Errors
   , ErrEncode (..)
   , ErrInput (..)
   , ErrDecode (..)
   , ErrOutput (..)
   , ErrStatement (..)
   , ErrRows (..)
   , ErrTransaction (..)

    -- * Re-exports
   , S.SQLData (..)
   , S.SQLVFS (..)
   )
where

import Control.Exception.Safe qualified as Ex
import Control.Monad hiding (foldM)
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource qualified as R
import Control.Monad.Trans.Resource.Extra qualified as R
import Data.Acquire qualified as A
import Data.Function
import Database.SQLite3 qualified as S
import Di.Df1 qualified as Di
import System.FilePath
import Prelude hiding (Read, maybe, read)

import Sq.Connection
import Sq.Decoders
import Sq.Encoders
import Sq.Input
import Sq.Migrations
import Sq.Mode
import Sq.Names
import Sq.Null
import Sq.Output
import Sq.Pool
import Sq.Statement
import Sq.Support
import Sq.Transactional

--------------------------------------------------------------------------------

-- $resources
--
-- "Sq" relies heavily on 'A.Acquire' for safe resource management in light of
-- concurrency and dependencies between resources.
--
-- As a user of "Sq", you mostly just have to figure out how to obtain a 'Pool'.
-- For that, you will probably benefit use one of these functions:
--
-- * 'with' for integrating with 'Ex.MonadMask' from the @exceptions@ library.
--
-- * 'new' for integrating with 'R.MonadResource' from the @resourcet@ library.
--
-- * 'uith' for integrating with 'R.MonadUnliftIO' from the @unliftio@ library.
--
-- If you have no idea what I'm talking about, just use 'with'.
-- Here is an example:
--
-- @
-- 'with' 'tempPool' \\(__pool__ :: 'Pool' \''Write') ->
--     /-- Here use __pool__ as necessary./
--     /-- The resources associated with it will be/
--     /-- automatically released after leaving this scope./
-- @
--
-- Now that you have a 'Pool', try to solve your problems within
-- 'Transactional' by means of 'Sq.read', 'Sq.commit' or 'Sq.rollback'.
--
-- However, if you need to interleave 'IO' actions while streaming result rows
-- out of the database, 'Transactional' won't be enough. You will need to use
-- 'foldIO' or 'streamIO'.

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
-- * Use "Di".'Di.new' to obtain the 'Di.Df1' parameter. Consider using
-- "Di.Core".'Di.Core.filter' to filter-out excessive logging. For example:
--
--       @"Di.Core".'Di.Core.filter' \\l _ _ -> l >= "Df1".'Df1.Info'@
tempPool :: Di.Df1 -> A.Acquire (Pool Write)
tempPool di0 = do
   d <- acquireTmpDir
   let path = d </> "db.sqlite"
       di1 = Di.push "sq" di0
   Di.notice di1 $ "Using temporary database: " <> show path
   pool SWrite di1 $ settings path

-- | Acquire a read-'Write' 'Pool' according to the given 'Settings'.
--
-- * Use "Di".'Di.new' to obtain the 'Di.Df1' parameter. Consider using
-- "Di.Core".'Di.Core.filter' to filter-out excessive logging. For example:
--
--       @"Di.Core".'Di.Core.filter' \\l _ _ -> l >= "Df1".'Df1.Info'@
writePool :: Di.Df1 -> Settings -> A.Acquire (Pool Write)
writePool di0 = pool SWrite (Di.push "sq" di0)
{-# INLINE writePool #-}

-- | Acquire a 'Read'-only 'Pool' according to the given 'Settings'.
--
-- * Use "Di".'Di.new' to obtain the 'Di.Df1' parameter. Consider using
-- "Di.Core".'Di.Core.filter' to filter-out excessive logging. For example:
--
--       @"Di.Core".'Di.Core.filter' \\l _ _ -> l >= "Df1".'Df1.Info'@
readPool :: Di.Df1 -> Settings -> A.Acquire (Pool Read)
readPool di0 = pool SRead (Di.push "sq" di0)
{-# INLINE readPool #-}

--------------------------------------------------------------------------------

-- | Execute a 'Read'-only 'Transactional' in a fresh 'Transaction' that will
-- be automatically released when done.
read
   :: (MonadIO m, SubMode p 'Read)
   => Pool p
   -> (forall g. Transactional g 'Retry 'Read a)
   -> m a
read p = transactionalRetry $ readTransaction p
{-# INLINE read #-}

-- | Execute a read-'Write' 'Transactional' in a fresh 'Transaction' that will
-- be automatically committed when done.
commit
   :: (MonadIO m)
   => Pool 'Write
   -> (forall g. Transactional g 'Retry 'Write a)
   -> m a
commit p = transactionalRetry $ commitTransaction p
{-# INLINE commit #-}

-- | Execute a read-'Write' 'Transactional' in a fresh 'Transaction' that will
-- be automatically rolled-back when done.
--
-- __This is mostly useful for testing__.
rollback
   :: (MonadIO m)
   => Pool 'Write
   -> (forall g. Transactional g 'Retry 'Write a)
   -> m a
rollback p = transactionalRetry $ rollbackTransaction p
{-# INLINE rollback #-}

--------------------------------------------------------------------------------

-- $migrations
--
-- 1. List all the 'Migration's in chronological order.
--    Each 'Migration' is a 'Transactional' action on the database, identified
--    by a unique 'MigrationId'. Construct with 'migration'.
--
--     @
--     __migrations__ :: ["Sq".'Migration']
--     __migrations__ =
--        [ "Sq".'migration' /\"create users table\"/ createUsersTable
--        , "Sq".'migration' /\"add email column to users\"/ addUserEmailColumn
--        , "Sq".'migration' /\"create articles table\"/ createArticlesTable
--        , / ... more migrations ... /
--        ]
--     @
--
-- 2. Run any 'Migration's that haven't been run yet, if necessary, by performing
--    'migrate' once as soon as you obtain your 'Write' connection 'Pool'.
--    'migrate' will enforce that the 'MigrationId's, be unique, and will
--    make sure that any migration history in the 'MigrationsTable' is
--    compatible with the specified 'Migration's.
--
--     @
--     "Sq".'migrate' pool /\"migrations\"/ __migrations__ \\case
--        []   -> /... No migrations will run. .../
--        mIds -> /... Some migrations will run. Maybe backup things here? .../
--     @
--
-- 3. __Don't change your 'MigrationId's over time__. If you do, then the
--    history in 'MigrationsTable' will become unrecognizable by 'migrate'.
--    Also, avoid having the 'Transactional' code in each 'Migration' use your
--    domain types and functions, as doing so may force you to alter past
--    'Transactional' if your domain types and functions change. Ideally,
--    you should write each 'Migration' in such a way that you never /have/
--    to modify them in the future.
