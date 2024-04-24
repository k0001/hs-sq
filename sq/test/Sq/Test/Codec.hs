module Sq.Test.Codec (tree) where

import Control.Exception.Safe qualified as Ex
import Control.Monad.IO.Class
import Data.Aeson qualified as Ae
import Data.Binary qualified as Bin
import Data.Bits
import Data.ByteString qualified as B
import Data.ByteString.Lazy qualified as BL
import Data.Fixed
import Data.Functor.Contravariant
import Data.Int
import Data.Maybe
import Data.Text qualified as T
import Data.Text.Lazy qualified as TL
import Data.Time qualified as Time
import Data.Time.Clock.POSIX qualified as Time
import Data.Time.Format.ISO8601 qualified as Time
import Data.Typeable
import Data.Word
import Hedgehog qualified as H
import Hedgehog.Gen qualified as H
import Hedgehog.Range qualified as HR
import Numeric.Natural
import Sq qualified
import Test.Tasty (testGroup)
import Test.Tasty.Hedgehog (testProperty)
import Test.Tasty.Runners (TestTree)

--------------------------------------------------------------------------------

tree :: IO (Sq.Pool Sq.Write) -> TestTree
tree iop =
   testGroup
      "decode . encode"
      [ t @Bool $ H.bool
      , t @Int $ H.integral HR.constantBounded
      , t @Int8 $ H.integral HR.constantBounded
      , t @Int16 $ H.integral HR.constantBounded
      , t @Int32 $ H.integral HR.constantBounded
      , t @Int64 $ H.integral HR.constantBounded
      , t @Word $ H.integral HR.constantBounded
      , t @Word8 $ H.integral HR.constantBounded
      , t @Word16 $ H.integral HR.constantBounded
      , t @Word32 $ H.integral HR.constantBounded
      , t @Word64 $ H.integral HR.constantBounded
      , t @Word64 $ H.integral HR.constantBounded
      , t @Natural $ H.integral $ HR.constant 0 maxNatural
      , t @Integer $ H.integral $ HR.constantFrom 0 minInteger maxInteger
      , t @Char H.unicode
      , t @String $ H.string (HR.constant 0 50) H.unicode
      , t @T.Text $ H.text (HR.constant 0 50) H.unicode
      , t @TL.Text $ fmap TL.fromStrict $ H.text (HR.constant 0 50) H.unicode
      , t2 @B.ByteString $ H.bytes (HR.constant 0 50)
      , t2 @BL.ByteString $ fmap BL.fromStrict $ H.bytes (HR.constant 0 50)
      , t2 @Time.UTCTime $ genUTCTime (HR.constantFrom epochUTCTime minUTCTime maxUTCTime)
      , t @Double $ H.double (HR.constantFrom 0 (fromIntegral minInteger) (fromIntegral maxInteger))
      , t @Float $ H.float (HR.constantFrom 0 (fromIntegral minInteger) (fromIntegral maxInteger))
      -- TODO FAIL: , testProperty "Char" $ t @Char (pure '\55296')
      ]
  where
   t
      :: forall a
       . ( Typeable a
         , Eq a
         , Show a
         , Sq.EncodeDefault a
         , Sq.DecodeDefault a
         , Bin.Binary a
         , Ae.ToJSON a
         , Ae.FromJSON a
         )
      => H.Gen a
      -> TestTree
   t ga =
      testGroup
         (tyConName (typeRepTyCon (typeRep ga)))
         [ testGroup "raw" [t2 ga]
         , testGroup "binary" [t2 (WrapBinary <$> ga)]
         , testGroup "aeson" [t2 (WrapAeson <$> ga)]
         ]

   t2
      :: forall a
       . (Typeable a, Eq a, Show a, Sq.EncodeDefault a, Sq.DecodeDefault a)
      => H.Gen a
      -> TestTree
   t2 ga =
      testGroup
         (tyConName (typeRepTyCon (typeRep ga)))
         [ testProperty "pure" $ H.property do
            a0 <- H.forAll ga
            let Sq.Encode g = Sq.encodeDefault
            case g a0 of
               Left e0 -> Ex.throwM e0
               Right raw -> do
                  let Sq.Decode f = Sq.decodeDefault
                  case f raw of
                     Left e -> Ex.throwM e
                     Right a1 -> a0 H.=== a1
         , testProperty "db" $ H.property do
            p <- liftIO iop
            a0 <- H.forAll ga
            a1 <- Sq.read p $ Sq.one idStatement a0
            a0 H.=== a1
         ]

newtype WrapBinary a = WrapBinary a
   deriving newtype (Eq, Show)

instance (Bin.Binary a) => Sq.EncodeDefault (WrapBinary a) where
   encodeDefault = contramap (\case WrapBinary a -> a) $ Sq.encodeBinary Bin.put

instance (Bin.Binary a) => Sq.DecodeDefault (WrapBinary a) where
   decodeDefault = WrapBinary <$> Sq.decodeBinary Bin.get

newtype WrapAeson a = WrapAeson a
   deriving newtype (Eq, Show)

instance (Ae.ToJSON a) => Sq.EncodeDefault (WrapAeson a) where
   encodeDefault =
      contramap (\case WrapAeson a -> a) $ Sq.encodeAeson Ae.toJSON

instance (Ae.FromJSON a) => Sq.DecodeDefault (WrapAeson a) where
   decodeDefault = WrapAeson <$> Sq.decodeAeson Ae.parseJSON

idStatement
   :: (Sq.EncodeDefault x, Sq.DecodeDefault x)
   => Sq.Statement Sq.Read x x
idStatement =
   Sq.readStatement
      (Sq.input "a" (Sq.input "b" "c"))
      (Sq.output "x" (Sq.output "y" "z"))
      "SELECT $a__b__c AS x__y__z"

maxNatural :: Natural
maxNatural = 2 ^ (256 :: Int) - 1

maxInteger :: Integer
maxInteger = 2 ^ (255 :: Int) - 1

minInteger :: Integer
minInteger = complement maxInteger

minUTCTime :: Time.UTCTime
minUTCTime = fromJust $ Time.iso8601ParseM "-9999-01-01T00:00:00Z"

maxUTCTime :: Time.UTCTime
maxUTCTime = fromJust $ Time.iso8601ParseM "9999-12-31T24:00:00Z"

epochUTCTime :: Time.UTCTime
epochUTCTime = posixPicoSecondsToUTCTime 0

genUTCTime :: (H.MonadGen m) => H.Range Time.UTCTime -> m Time.UTCTime
genUTCTime =
   fmap posixPicoSecondsToUTCTime
      . H.integral
      . fmap utcTimeToPOSIXPicoSeconds

utcTimeToPOSIXPicoSeconds :: Time.UTCTime -> Integer
utcTimeToPOSIXPicoSeconds t = i
  where
   MkFixed i = Time.nominalDiffTimeToSeconds $ Time.utcTimeToPOSIXSeconds t

posixPicoSecondsToUTCTime :: Integer -> Time.UTCTime
posixPicoSecondsToUTCTime =
   Time.posixSecondsToUTCTime . Time.secondsToNominalDiffTime . MkFixed

-- genRational :: (H.MonadGen m) => m Rational
-- genRational = do
--    n <- genInteger
--    d <- H.integral $ H.linear 1 (10 ^ (10 :: Int))
--    pure (n % d)
