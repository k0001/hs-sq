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
import Control.Monad.Trans.Resource.Extra qualified as R
import Data.Acquire qualified as A
import Data.Acquire.Internal qualified as A
import Data.Pool qualified as P
import Data.Singletons
import GHC.Records
import Prelude hiding (Read, log, read)

import Sq.Connection
import Sq.Mode

--------------------------------------------------------------------------------

data Pool (mode :: Mode) where
   Pool_Read
      :: P.Pool (A.Allocated (Connection Read))
      -> Pool Read
   Pool_Write
      :: Connection Write
      -> P.Pool (A.Allocated (Connection Read))
      -> Pool Write

instance NFData (Pool mode) where
   rnf (Pool_Read !_) = ()
   rnf (Pool_Write a !_) = rnf a

instance
   HasField
      "read"
      (Pool mode)
      (A.Acquire (Transaction Reading))
   where
   getField = read

instance
   HasField
      "commit"
      (Pool Write)
      (A.Acquire (Transaction Committing))
   where
   getField = commit

instance
   HasField
      "rollback"
      (Pool Write)
      (A.Acquire (Transaction Rollbacking))
   where
   getField = rollback

pool :: forall mode. (SingI mode) => Settings -> A.Acquire (Pool mode)
pool cs = do
   case sing @mode of
      SRead -> Pool_Read <$> acquirePPoolRead
      SWrite -> Pool_Write <$> acquireConnection cs <*> acquirePPoolRead
  where
   acquirePPoolRead :: A.Acquire (P.Pool (A.Allocated (Connection Read)))
   acquirePPoolRead =
      R.acquire1
         ( \res -> do
            let A.Acquire f = acquireConnection cs
            maxResources <- max 8 <$> getNumCapabilities
            P.newPool $
               P.defaultPoolConfig
                  (f res)
                  (\(A.Allocated _ g) -> g A.ReleaseNormal)
                  (60 {- timeout seconds -})
                  maxResources
         )
         P.destroyAllResources

read :: Pool mode -> A.Acquire (Transaction Reading)
read p = acquirePoolConnectionRead p >>= acquireTransaction

commit :: Pool Write -> A.Acquire (Transaction Committing)
commit (Pool_Write c _) = acquireTransaction c

rollback :: Pool Write -> A.Acquire (Transaction Rollbacking)
rollback (Pool_Write c _) = acquireTransaction c

acquirePoolConnectionRead :: Pool mode -> A.Acquire (Connection Read)
acquirePoolConnectionRead p = do
   let ppr = case p of Pool_Write _ x -> x; Pool_Read x -> x
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
