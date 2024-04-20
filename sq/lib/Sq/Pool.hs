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
import Data.Singletons
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

data Pool (mode :: Mode) where
   Pool_Read
      :: PoolId
      -> P.Pool (A.Allocated (Connection Read))
      -> Pool Read
   Pool_Write
      :: PoolId
      -> Connection Write
      -> P.Pool (A.Allocated (Connection Read))
      -> Pool Write

instance NFData (Pool mode) where
   rnf (Pool_Read !_ !_) = ()
   rnf (Pool_Write !_ a !_) = rnf a

instance HasField "id" (Pool mode) PoolId where
   getField = \case
      Pool_Read x _ -> x
      Pool_Write x _ _ -> x

instance
   HasField
      "read"
      (Pool mode)
      (A.Acquire (Transaction Read))
   where
   getField = read

instance
   HasField
      "commit"
      (Pool Write)
      (A.Acquire (Transaction Write))
   where
   getField = commit

instance
   HasField
      "rollback"
      (Pool Write)
      (A.Acquire (Transaction Write))
   where
   getField = rollback

pool
   :: forall mode
    . (SingI mode)
   => Di.Df1
   -> Settings
   -> A.Acquire (Pool mode)
pool di0 cs = do
   pId <- newPoolId
   let di1 = Di.attr "id" pId di0
   case sing @mode of
      SRead -> Pool_Read pId <$> ppoolConnRead di1
      SWrite -> Pool_Write pId <$> connection di1 cs <*> ppoolConnRead di1
  where
   ppoolConnRead
      :: Di.Df1 -> A.Acquire (P.Pool (A.Allocated (Connection Read)))
   ppoolConnRead di1 =
      R.acquire1
         ( \res -> do
            let A.Acquire f = connection di1 cs
            maxResources <- max 8 <$> getNumCapabilities
            P.newPool $
               P.defaultPoolConfig
                  (f res)
                  (\(A.Allocated _ g) -> g A.ReleaseNormal)
                  (60 {- timeout seconds -})
                  maxResources
         )
         P.destroyAllResources

-- @'read' p == p.read@
read :: Pool mode -> A.Acquire (Transaction Read)
read p = poolConnectionRead p >>= readTransaction'

-- @'commit' p == p.commit@
commit :: Pool Write -> A.Acquire (Transaction Write)
commit (Pool_Write _ c _) = writeTransaction' Commit c

-- @'rollback' p == p.rollback@
rollback :: Pool Write -> A.Acquire (Transaction Write)
rollback (Pool_Write _ c _) = writeTransaction' Rollback c

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
