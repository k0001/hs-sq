module Main (main) where

import Df1 qualified
import Di qualified
import Di.Core qualified
import Sq.Test qualified
import Test.Tasty qualified as Tasty
import Test.Tasty.Hedgehog qualified as Tasty
import Test.Tasty.Runners qualified as Tasty

--------------------------------------------------------------------------------

main :: IO ()
main = Di.new \di0 -> do
   let di1 = Di.Core.filter (\l _ _ -> l >= Df1.Info) di0
   Tasty.defaultMainWithIngredients
      [ Tasty.consoleTestReporter
      , Tasty.listingTests
      ]
      $ Tasty.localOption (Tasty.HedgehogTestLimit (Just 1000))
      $ Sq.Test.tree di1
