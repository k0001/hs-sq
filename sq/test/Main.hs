module Main (main) where

import Sq.Test qualified
import Test.Tasty qualified as Tasty
import Test.Tasty.Hedgehog qualified as Tasty
import Test.Tasty.Runners qualified as Tasty

--------------------------------------------------------------------------------

main :: IO ()
main =
   Tasty.defaultMainWithIngredients
      [ Tasty.consoleTestReporter
      , Tasty.listingTests
      ]
      $ Tasty.localOption (Tasty.HedgehogTestLimit (Just 1000))
      $ Sq.Test.tree
