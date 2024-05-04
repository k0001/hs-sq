module Sq.Migrations {--}
   ( migrate
   , migration
   , Migration
   , MigrationId
   , MigrationsTable
   ) -- }
where

import Control.Exception.Safe qualified as Ex
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource.Extra qualified as R
import Data.Aeson qualified as Ae
import Data.List qualified as List
import Data.Set (Set)
import Data.Set qualified as Set
import Data.String
import Data.Text qualified as T
import Data.Time qualified as Time
import GHC.Records

import Sq.Connection
import Sq.Decoders
import Sq.Encoders
import Sq.Mode
import Sq.Names
import Sq.Pool
import Sq.Statement
import Sq.Transactional

-- | Name of the database table keeping a registry of executed 'Migration's, by
-- their 'MigrationId'.
--
-- * Same syntax rules as 'Name'.
--
-- * This table will be created and updated by 'migrate' as necessary.
newtype MigrationsTable = MigrationsTable Name
   deriving newtype (Eq, Ord, Show, IsString, Ae.FromJSON, Ae.ToJSON)

instance HasField "text" MigrationsTable T.Text where
   getField (MigrationsTable x) = x.text

-- | A single 'Migration' consisting of a 'Transactional' action uniquely
-- identified by a 'MigrationId'.
--
-- * Construct with 'migration'.
--
-- * Run through 'migrate'.
data Migration = Migration MigrationId (Transaction 'Write -> IO ())

instance HasField "id" Migration MigrationId where
   getField (Migration x _) = x

-- | 'Just' if at least one 'MigrationId' is duplicate.
duplicateMigrationId :: [Migration] -> Maybe MigrationId
duplicateMigrationId = go mempty
  where
   go :: Set MigrationId -> [Migration] -> Maybe MigrationId
   go !mIds = \case
      Migration mId _ : ms
         | Set.member mId mIds -> Just mId
         | otherwise -> go (Set.insert mId mIds) ms
      [] -> Nothing

-- | Define a single 'Migration' that, when executed, will perform
-- the given 'Transactional'.
--
-- * See 'Migration'.
migration
   :: MigrationId
   -> (forall g. Transactional g 'NoRetry 'Write ())
   -- ^ Notice the 'NoRetry'. In other words, this 'Transactional'
   -- can't perform 'retry' nor any 'Control.Applicative.Alternative'
   -- nor 'MonadPlus' features.
   -> Migration
migration mId ta = Migration mId \tx -> embed tx ta

-- | Unique identifier for a 'Migration' within a 'MigrationsTable'.
--
-- * You are supposed to type these statically, so construct a 'MigrationId'
-- by typing down the literal string.
newtype MigrationId = MigrationId T.Text
   deriving newtype (Eq, Ord, IsString, Show, EncodeDefault, DecodeDefault)

instance HasField "text" MigrationId T.Text where
   getField (MigrationId x) = x

--------------------------------------------------------------------------------

createMigrationsTable :: MigrationsTable -> Statement 'Write () ()
createMigrationsTable tbl =
   -- We are storing the timestamp in case we need it in the future.
   -- We aren't really using it now.
   writeStatement mempty mempty $
      fromString $
         "CREATE TABLE IF NOT EXISTS "
            <> show tbl
            <> " (ord INTEGER PRIMARY KEY NOT NULL CHECK (ord >= 0)"
            <> ", id TEXT UNIQUE NOT NULL"
            <> ", time TEXT NOT NULL)"

getMigrations
   :: MigrationsTable -> Statement 'Read () (Time.UTCTime, MigrationId)
getMigrations tbl =
   readStatement mempty (liftA2 (,) "time" "id") $
      fromString $
         "SELECT time, id FROM " <> show tbl <> " ORDER BY ord ASC"

pushMigration :: MigrationsTable -> Statement 'Write MigrationId Time.UTCTime
pushMigration tbl =
   writeStatement "id" "time" $
      fromString $
         "INSERT INTO "
            <> show tbl
            <> " (ord, time, id)"
            <> " SELECT t.ord"
            <> ", strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now', 'subsecond')"
            <> ", $id"
            <> " FROM (SELECT coalesce(max(ord) + 1, 0) AS ord FROM "
            <> show tbl
            <> ") AS t"
            <> " RETURNING time"

--------------------------------------------------------------------------------

-- | Run all the migrations in 'Migration's that haven't been run yet.
--
-- * If the 'MigrationId's are not compatible with the current migration
-- history as reported by 'MigrationsTable', there will be an exception.
--
-- * If 'MigrationsTable' doesn't exist, it will be created.
--
-- * All the changes are run in a single 'Transaction', including those to
-- 'MigrationsTable'.
migrate
   :: forall a m
    . (MonadIO m, Ex.MonadMask m)
   => Pool 'Write
   -- ^ Connection 'Pool' to the database to migrate.
   -> MigrationsTable
   -- ^ Name of the table where the registry of ran 'Migration's is kept.
   -> [Migration]
   -- ^ 'Migration's to apply to the database, if necessary, in chronological
   -- order.
   -> ([MigrationId] -> m a)
   -- ^ This will be performed __while the write transaction is active__,
   -- letting you know which 'MigrationId's are to be performed, and in which
   -- order.
   --
   -- This can be a good place to perform a backup of the database if
   -- necessary. Presumably, 'migrate' is being run during the initialization
   -- of your program, suggesting that nobody else is trying to write to the
   -- database at this time, so it's OK if this code takes a while to run.
   --
   -- Don't try to acquire a 'Write' transaction here, it will deadlock.
   -- It's OK to interact with the 'Pool' through 'Read'-only means.
   -> m a
migrate p tbl want k
   | Just mId <- duplicateMigrationId want =
      Ex.throwString $ "Duplicate migration id: " <> show mId
   | otherwise = R.withAcquire (commitTransaction p) \tx -> do
      pending <- embed tx do
         zero (createMigrationsTable tbl) ()
         (nran, ran) <- list (getMigrations tbl) ()
         case List.stripPrefix (fmap snd ran) (fmap (.id) want) of
            Just _ -> pure $ List.drop (fromIntegral nran) want
            Nothing ->
               Ex.throwString $
                  "Incompatible migration history: " <> show (fmap snd ran)
      a <- k $ fmap (.id) pending
      forM_ pending \(Migration mId f) -> do
         liftIO $ f tx
         void $ embed tx $ one (pushMigration tbl) mId
      pure a
