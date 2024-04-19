module Main (main) where

import Control.Concurrent
import Control.Concurrent.Async qualified as Async
import Control.Exception.Safe qualified as Ex
import Control.Monad
import Control.Monad.Trans.Resource.Extra qualified as R
import Criterion.Main
import Data.Acquire qualified as A
import Data.IORef
import Sq qualified

digitChar :: Int -> Char
digitChar i = case show i of
   [c] -> c
   _ -> error "digitChar"

{-
Something more or less like this will be printed. Notice how the inserts (I),
reads (0..9) and deletes (D) are nicely interleaved. This means that work
proceeds as soon as possible.

I1394860527I2743590618I2179540638I0314589267I1204956738I6083247951I8352640917I10
45968732I13290754I601978235468DDDI02136459I87360157482I675038149I9326257081D49I1
374256908DI104963725I0431825769DI1370594826D8I5493728061DI7901654382DDI06172354I
4095891726I1390465738I0257846392D8D1DDI7241506839I1657294038DI15897230I464701698
I35723620985D41I0426189753I5961273480DDDI102435798D6I0617984352I932056481I729386
7154D0I4263091785I7036254198I4806325917DDDI3584976210DDDDDDDI987561203D4I0178953
426I2381674590I302178469D5I8239175406DDI290156347D8I3875210946DI05247638D91I1325
I6270147058963D489DI8019243675I3159642087I7693258140DI6513029847DDI0412789I36503
92715648I9685423071DI483672901I3092875154D6I5179623408DI5106938742I1497608235I02
59413768I3810954267DI5690378142I4052139867I6714820953DDDDI5209364817DI3861945207
DDDDDDDI0284195673I1302794865DI7910456328I1296380754I1306524897DI9103825647I2459
130867I1650432987I2061973854I1389064275DI9516847023I0293857146I0638271594I362987
4510DI0812376459I0345689271I9638417205DI950142786D3DDI2304795816I107263945D8I127
5940638DDI4318926705I782051934DD6DDDI741590I0763541386DD282D9DI0673129548DDDDDI5
841962037DDDI4286109375I1479608253DDI8653204917DDDDI4019735682DDDDDI9163270584DD

-}
run
   :: Sq.Pool Sq.Write
   -> Int
   -- ^ Number of data to be inserted. It will be read back 10 times.
   -> Benchmark
run pool nw = bench (show nw) $ nfIO do
   caps :: Int <- getNumCapabilities
   semForkW :: QSem <- newQSem (2 * caps)
   semForkR :: QSem <- newQSem (10 * caps)
   semDone :: QSemN <- newQSemN 0
   iorOrder :: IORef String <- newIORef ""
   let add :: Char -> IO ()
       add c = atomicModifyIORef' iorOrder \s -> (c : s, ())
   forM_ [1 .. nw] \i -> do
      waitQSem semForkW
      Async.link =<< Async.async do
         o1 <- Sq.row pool.commit stInsert i <* add 'I'
         when (o1 /= i) $ Ex.throwString "stInsert: o /= i"
         void $
            waitAll =<< forM [0 .. 9] \(j :: Int) -> do
               waitQSem semForkR
               Async.async do
                  o2 <- Sq.row pool.read stRead i <* add (digitChar j)
                  when (o2 /= i) $ Ex.throwString "stRead: o /= i"
                  signalQSem semForkR
         o3 <- Sq.row pool.commit stDelete i <* add 'D'
         when (o3 /= i) $ Ex.throwString "stDelete: o /= i"
         signalQSemN semDone 1
         signalQSem semForkW
   waitQSemN semDone nw
   putStrLn =<< fmap reverse (readIORef iorOrder)

waitAll :: [Async.Async a] -> IO [a]
waitAll = go []
  where
   go !acc (aa : rest) = Async.wait aa >>= \a -> go (a : acc) rest
   go !acc [] = pure $ reverse acc

stCreate :: Sq.Statement Sq.Write () ()
stCreate = Sq.writeStatement mempty mempty "CREATE TABLE t (x INTEGER)"

stInsert :: Sq.Statement Sq.Write Int Int
stInsert = Sq.writeStatement "i" "x" "INSERT INTO t (x) VALUES ($i) RETURNING x"

stRead :: Sq.Statement Sq.Read Int Int
stRead = Sq.readStatement "i" "x" "SELECT x FROM t WHERE x=$i"

stDelete :: Sq.Statement Sq.Write Int Int
stDelete = Sq.writeStatement "i" "x" "DELETE FROM t WHERE x=$i RETURNING x"

withPoolTmp :: (Sq.Pool Sq.Write -> Benchmark) -> Benchmark
withPoolTmp k =
   envWithCleanup
      ( do
         (pool, rel) <- R.withRestoreIO (R.unAcquire Sq.tempPool)
         Sq.rowsZero pool.commit stCreate ()
         pure (pool, rel)
      )
      (\(_, rel) -> rel A.ReleaseNormal)
      (\ ~(pool, _) -> k pool)

main :: IO ()
main =
   defaultMain
      [ bgroup
         "sq"
         [ withPoolTmp \ ~pool -> run pool 100
         ]
      ]
