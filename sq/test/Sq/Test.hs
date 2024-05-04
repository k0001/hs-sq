module Sq.Test (tree) where

import Control.Concurrent
import Control.Concurrent.Async qualified as Async
import Control.Exception.Safe qualified as Ex
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Ref
import Control.Monad.Trans.Resource.Extra qualified as R
import Data.Acquire.Internal qualified as A
import Data.Foldable
import Di qualified
import Hedgehog qualified as H
import Hedgehog.Gen qualified as H
import Hedgehog.Range qualified as HR
import System.Timeout
import Test.Tasty (testGroup, withResource)
import Test.Tasty.HUnit (testCase, (@?=))
import Test.Tasty.Hedgehog (testProperty)
import Test.Tasty.Runners (TestTree)

import Sq qualified
import Sq.Test.Codec qualified

--------------------------------------------------------------------------------

tree :: Di.Df1 -> TestTree
tree di = withAcquire (Sq.tempPool di) \iop ->
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
         (ysLen, ys) <- Sq.rollback pool do
            Sq.zero stCreate ()
            traverse_ (Sq.zero stInsert) xs
            Sq.list stRead ()
         ysLen H.=== fromIntegral (length ys)
         xs H.=== ys
      , testExample1 di
      , testMigs di
      ]

withAcquire :: A.Acquire a -> (IO a -> TestTree) -> TestTree
withAcquire acq k =
   withResource
      (R.withRestoreIO (R.unAcquire acq))
      (\(_, rel) -> rel A.ReleaseNormal)
      (k . fmap fst)

--------------------------------------------------------------------------------

data FakeException123 = FakeException123
   deriving stock (Show)
   deriving anyclass (Ex.Exception)

example1 :: Sq.Pool Sq.Write -> IO ()
example1 pool = Sq.commit pool do
   userId1 <- Sq.one insertUser "haskell@example.com"
   userId2 <- Sq.one getUserIdByEmail "haskell@example.com"
   when (userId1 /= userId2) do
      fail "Something unexpected happened!"
   ref <- newRef (0 :: Int)
   _userId3 <-
      Ex.catch
         ( do
            modifyRef ref (+ 1)
            _ <- Sq.one insertUser "sqlite@example.com"
            Ex.throwM FakeException123
         )
         ( \FakeException123 -> do
            Sq.zero getUserIdByEmail "sqlite@example.com"
            modifyRef ref (+ 10)
         )
   10 <- readRef ref
   Sq.zero getUserIdByEmail "sqlite@example.com"
   _userId4 <-
      Sq.maybe getUserIdByEmail "nix@example.com" >>= \case
         Just userId4 -> pure userId4
         Nothing -> mzero
   False <-
      mplus
         (writeRef ref 8 >> mzero >> pure True)
         (pure False)
   10 <- readRef ref
   pure ()

getUserIdByEmail :: Sq.Statement 'Sq.Read String Int
getUserIdByEmail =
   Sq.readStatement "email" "id" "SELECT id FROM users WHERE email = $email"

insertUser :: Sq.Statement 'Sq.Write String Int
insertUser =
   Sq.writeStatement
      "email"
      "id"
      "INSERT INTO users (email) VALUES ($email) RETURNING id"

createTableUsers :: Sq.Statement 'Sq.Write () ()
createTableUsers =
   Sq.writeStatement
      mempty
      mempty
      "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)"

testExample1 :: Di.Df1 -> TestTree
testExample1 di0 = testCase "example1" do
   Sq.with (Sq.tempPool di0) \pool -> do
      Sq.commit pool $ Sq.zero createTableUsers ()
      Async.withAsync
         ( do
            threadDelay 50_000
            void $ Sq.commit pool $ Sq.one insertUser "nix@example.com"
         )
         \_ ->
            timeout 500_000 (example1 pool) >>= \case
               Just () -> pure ()
               Nothing -> fail "example1: timeout!"

--------------------------------------------------------------------------------

testMigs :: Di.Df1 -> TestTree
testMigs di0 = testCase "migs" do
   Sq.with (Sq.tempPool di0) \pool -> do
      Sq.migrate pool "migs" migsAB \ids -> ids @?= ["A", "B"]
      Sq.migrate pool "migs" migsAB \ids -> ids @?= []
      for_ [migsCD, []] \migs ->
         Ex.catchJust
            ( \case
               Ex.StringException m _ ->
                  guard $ m == "Incompatible migration history: [\"A\",\"B\"]"
            )
            (Sq.migrate pool "migs" migs mempty)
            pure
      Sq.migrate pool "migs" migsAB \ids -> ids @?= []
      Sq.migrate pool "migs" (migsAB <> migsCD) \ids -> ids @?= ["C", "D"]
      Sq.migrate pool "migs" (migsAB <> migsCD) \ids -> ids @?= []
      Sq.read pool do
         (2, [7, 8 :: Int]) <-
            Sq.list
               (Sq.readStatement mempty "x" "SELECT x FROM t ORDER BY x ASC")
               ()
         pure ()
  where
   migsAB :: [Sq.Migration]
   migsAB =
      [ Sq.migration "A" (pure ())
      , Sq.migration "B" (pure ())
      ]
   migsCD :: [Sq.Migration]
   migsCD =
      [ Sq.migration "C" do
         Sq.zero @()
            (Sq.writeStatement mempty mempty "CREATE TABLE t (x INTEGER)")
            ()
      , Sq.migration "D" do
         Sq.zero @()
            (Sq.writeStatement mempty mempty "INSERT INTO t (x) VALUES (7), (8)")
            ()
      ]
