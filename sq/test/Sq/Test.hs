module Sq.Test (tree) where

import Control.Exception.Safe qualified as Ex
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource.Extra qualified as R
import Data.Acquire.Internal qualified as A
import Data.Foldable
import Data.Text qualified as T
import Di qualified
import Hedgehog qualified as H
import Hedgehog.Gen qualified as H
import Hedgehog.Range qualified as HR
import Sq qualified
import Test.Tasty (testGroup, withResource)
import Test.Tasty.HUnit (testCase, (@?=))
import Test.Tasty.Hedgehog (testProperty)
import Test.Tasty.Runners (TestTree)

import Sq.Test.Codec qualified

--------------------------------------------------------------------------------

tree :: Di.Df1 -> TestTree
tree di = withAcquire (Sq.poolTemp di) \iop ->
   testGroup
      "sq"
      [ {- This code randomly hangs for some reason. It seems to be related
           to HUnit and threading. Not sure.
                testGroup "Name" do
                    t0 <- ["a", "ab", "a1b", "a_b", "a1_b", "a_1b"]
                    t1 <- [t0, t0 <> "2"]
                    pure $ testCase (T.unpack t1) $ fmap (.text) (Sq.name t1) @?= Right t1
        -}
        Sq.Test.Codec.tree iop
      , testProperty "list" $ H.property do
         let stCreate =
               Sq.writeStatement @() @() mempty mempty $
                  "CREATE TABLE t (x INTEGER)"
             stInsert =
               Sq.writeStatement @Int @() "x" mempty $
                  "INSERT INTO t (x) VALUES ($x)"
             stRead = Sq.readStatement mempty "x" "SELECT x FROM t"
         pool <- liftIO iop
         xs :: [Int] <- H.forAll $ H.list (HR.constant 0 100) H.enumBounded
         (ysLen, ys) <- Sq.transactional pool.rollback do
            Sq.zero stCreate ()
            traverse_ (Sq.zero stInsert) xs
            Sq.list stRead ()
         ysLen H.=== fromIntegral (length ys)
         xs H.=== ys
      ]

withAcquire :: A.Acquire a -> (IO a -> TestTree) -> TestTree
withAcquire acq k =
   withResource
      (R.withRestoreIO (R.unAcquire acq))
      (\(_, rel) -> rel A.ReleaseNormal)
      (k . fmap fst)