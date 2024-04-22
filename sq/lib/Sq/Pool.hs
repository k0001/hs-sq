{-# LANGUAGE StrictData #-}
{-# LANGUAGE NoFieldSelectors #-}

module Sq.Pool
   ( Pool
   , pool
   , read
   , commit
   , rollback
   )
where

import Control.Concurrent
import Control.DeepSeq
import Control.Exception.Safe qualified as Ex
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource.Extra qualified as R
import Data.Acquire qualified as A
import Data.Acquire.Internal qualified as A
import Data.Pool qualified as P
import Data.Word
import Di.Df1 qualified as Di
import GHC.Records
import Prelude hiding (Read, log, read)

import Sq.Connection
import Sq.Mode
import Sq.Support

--------------------------------------------------------------------------------

newtype PoolId = PoolId Word64
   deriving newtype (Eq, Ord, Show, NFData, Di.ToValue)

newPoolId :: (MonadIO m) => m PoolId
newPoolId = PoolId <$> newUnique

--------------------------------------------------------------------------------

-- | Pool of connections to a SQLite database.
--
-- * @p@ indicates whether 'Read'-only or read-'Write' 'Statement's are
-- supported by this 'Pool'.
--
-- * Obtain with 'Sq.readPool', 'Sq.writePool' or 'Sq.tempPool'.
--
-- * It's safe and efficient to use a 'Pool' concurrently as is.
-- Concurrency is handled internally.
data Pool (p :: Mode) where
   Pool_Read
      :: PoolId
      -> P.Pool (A.Allocated (Connection Read))
      -> Pool Read
   Pool_Write
      :: PoolId
      -> Connection Write
      -> P.Pool (A.Allocated (Connection Read))
      -> Pool Write

instance NFData (Pool p) where
   rnf (Pool_Read !_ !_) = ()
   rnf (Pool_Write !_ a !_) = rnf a

instance HasField "id" (Pool p) PoolId where
   getField = \case
      Pool_Read x _ -> x
      Pool_Write x _ _ -> x

-- | @'read' pool == pool.read@
instance HasField "read" (Pool p) (A.Acquire (Transaction Read)) where
   getField = read

-- | @'commit' pool == pool.commit@
instance HasField "commit" (Pool Write) (A.Acquire (Transaction Write)) where
   getField = commit

-- | @'rollback' pool == pool.rollback@
instance HasField "rollback" (Pool Write) (A.Acquire (Transaction Write)) where
   getField = rollback

pool :: SMode p -> Di.Df1 -> Settings -> A.Acquire (Pool p)
pool smode di0 cs = do
   pId <- newPoolId
   let di1 = Di.attr "id" pId di0
   ppcr <- ppoolConnRead di1
   case smode of
      SRead -> pure $ Pool_Read pId ppcr
      SWrite -> do
         cw <- connection SWrite di1 cs
         pure $ Pool_Write pId cw ppcr
  where
   ppoolConnRead
      :: Di.Df1 -> A.Acquire (P.Pool (A.Allocated (Connection Read)))
   ppoolConnRead di1 =
      R.acquire1
         ( \res -> do
            let A.Acquire f = connection SRead di1 cs
            maxResources <- max 8 <$> getNumCapabilities
            P.newPool $
               P.defaultPoolConfig
                  (f res)
                  (\(A.Allocated _ g) -> g A.ReleaseNormal)
                  (60 {- timeout seconds -})
                  maxResources
         )
         P.destroyAllResources

-- | Acquire a read-only transaction.
--
-- @'read' pool == pool.read@
read :: Pool mode -> A.Acquire (Transaction Read)
read p = poolConnectionRead p >>= readTransaction'

-- | Acquire a read-commit transaction where changes are finally commited to
-- the database unless there is an unhandled exception during the transaction,
-- in which case they are rolled back.
--
-- @'commit' pool == pool.commit@
commit :: Pool Write -> A.Acquire (Transaction Write)
commit (Pool_Write _ c _) = writeTransaction' True c

-- | Acquire a read-commit transaction where changes are always rolled back.
-- This is mostly useful for testing purposes.
--
-- Notice that an equivalent behavior can be achieved by
-- 'Control.Exception.Safe.bracket'ing changes between 'Sq.savepoint' and
-- 'Sq.rollbackTo' in a 'commit'ting transaction. Or by using 'Ex.throwM'
-- and 'Ex.catch' within 'Transactional'. However, using this 'rollback'
-- is much faster.
rollback :: Pool Write -> A.Acquire (Transaction Write)
rollback (Pool_Write _ c _) = writeTransaction' False c

poolConnectionRead :: Pool mode -> A.Acquire (Connection Read)
poolConnectionRead p = do
   let ppr = case p of Pool_Write _ _ x -> x; Pool_Read _ x -> x
   tid <- flip R.mkAcquire1 killThread $ forkIO $ forever do
      threadDelay 10_000_000
      putStrLn "Waited 10 seconds to acquire a database connection from the pool"
   fmap (\(A.Allocated c _, _) -> c) do
      R.mkAcquireType1
         (P.takeResource ppr <* killThread tid)
         \(a@(A.Allocated _ rel), lp) t -> case t of
            A.ReleaseExceptionWith _ ->
               rel t `Ex.finally` P.destroyResource ppr lp a
            _ -> P.putResource lp a
