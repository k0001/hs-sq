module Sq.Decoders
   ( Decode (..)
   , ErrDecode (..)
   , decodeRefine
   , DecodeDefault (..)
   , decodeMaybe
   , decodeEither
   , decodeNS
   , decodeSizedIntegral
   , decodeBinary
   , decodeBinary'
   , decodeRead
   , decodeAeson
   , decodeAeson'
   ) where

import Control.Applicative
import Control.Exception.Safe qualified as Ex
import Control.Monad
import Control.Monad.Catch qualified as Ex (MonadThrow (..))
import Control.Monad.Trans.Reader
import Data.Aeson qualified as Ae
import Data.Aeson.Types qualified as Ae
import Data.Bifunctor
import Data.Binary qualified as Bin
import Data.Binary.Get qualified as Bin
import Data.Bits
import Data.ByteString qualified as B
import Data.ByteString.Builder.Prim.Internal (caseWordSize_32_64)
import Data.ByteString.Lazy qualified as BL
import Data.Int
import Data.Proxy
import Data.SOP qualified as SOP
import Data.Text qualified as T
import Data.Text.Lazy qualified as TL
import Data.Text.Unsafe qualified as T
import Data.Time qualified as Time
import Data.Time.Clock.POSIX qualified as Time
import Data.Time.Format.ISO8601 qualified as Time
import Data.Word
import Database.SQLite3 qualified as S
import GHC.Float (double2Float, float2Double)
import GHC.Stack
import Numeric.Natural
import Text.Read (readEither, readMaybe)

import Sq.Null (Null)
import Sq.Support

--------------------------------------------------------------------------------

-- | How to decode a single SQLite column value into a Haskell value of type
-- @a@.
newtype Decode a
   = -- | Decode a 'S.SQLData' value into a value of type @a@.
     Decode (S.SQLData -> Either ErrDecode a)
   deriving
      (Functor, Applicative, Monad)
      via ReaderT S.SQLData (Either ErrDecode)

-- | @'mempty' = 'pure' 'mempty'@
instance (Monoid a) => Monoid (Decode a) where
   mempty = pure mempty
   {-# INLINE mempty #-}

-- | @('<>') == 'liftA2' ('<>')@
instance (Semigroup a) => Semigroup (Decode a) where
   (<>) = liftA2 (<>)
   {-# INLINE (<>) #-}

instance Ex.MonadThrow Decode where
   throwM = Decode . const . Left . ErrDecode_Fail . Ex.toException

instance MonadFail Decode where
   fail = Ex.throwString
   {-# INLINE fail #-}

-- | Leftmost result on success, rightmost error on failure.
instance Alternative Decode where
   empty = fail "empty"
   {-# INLINE empty #-}
   (<|>) = mplus
   {-# INLINE (<|>) #-}

-- | Leftmost result on success, rightmost error on failure.
instance MonadPlus Decode where
   mzero = fail "mzero"
   {-# INLINE mzero #-}
   mplus (Decode l) (Decode r) = Decode \s ->
      either (\_ -> r s) pure (l s)
   {-# INLINE mplus #-}

-- | See v'Decode'.
data ErrDecode
   = -- | Received 'S.ColumnType', expected 'S.ColumnType's.
     ErrDecode_Type S.ColumnType [S.ColumnType]
   | ErrDecode_Fail Ex.SomeException
   deriving stock (Show)
   deriving anyclass (Ex.Exception)

--------------------------------------------------------------------------------

sqlDataColumnType :: S.SQLData -> S.ColumnType
sqlDataColumnType = \case
   S.SQLInteger _ -> S.IntegerColumn
   S.SQLFloat _ -> S.FloatColumn
   S.SQLText _ -> S.TextColumn
   S.SQLBlob _ -> S.BlobColumn
   S.SQLNull -> S.NullColumn

--------------------------------------------------------------------------------

-- | A convenience function for refining a 'Decode'r through a function that
-- may fail with a 'String' error message. The 'CallStack' is preserved.
--
-- If you need a more sophisticated refinement, use the 'Decode' constructor.
decodeRefine
   :: (HasCallStack)
   => (a -> Either String b)
   -> Decode a
   -> Decode b
decodeRefine f (Decode g) = Decode \raw -> do
   a <- g raw
   case f a of
      Right b -> Right b
      Left s -> first ErrDecode_Fail (Ex.throwString s)

--------------------------------------------------------------------------------
-- Core decodes

-- | Default way to decode a SQLite value into a Haskell value of type @a@.
--
-- If there there exist also a 'Sq.EncodeDefault' instance for @a@, then it
-- must roundtrip with the 'Sq.DecodeDefault' instance for @a@.
class DecodeDefault a where
   -- | Default way to decode a SQLite value into a Haskell value of type @a@.
   decodeDefault :: Decode a

-- | Literal 'S.SQLData' 'Decode'.
instance DecodeDefault S.SQLData where
   decodeDefault = Decode Right
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn'.
instance DecodeDefault Int64 where
   decodeDefault = Decode \case
      S.SQLInteger x -> Right x
      x -> Left $ ErrDecode_Type (sqlDataColumnType x) [S.IntegerColumn]

-- | 'S.FloatColumn'.
instance DecodeDefault Double where
   decodeDefault = Decode \case
      S.SQLFloat x -> Right x
      x -> Left $ ErrDecode_Type (sqlDataColumnType x) [S.FloatColumn]

-- | 'S.TextColumn'.
instance DecodeDefault T.Text where
   decodeDefault = Decode \case
      S.SQLText x -> Right x
      x -> Left $ ErrDecode_Type (sqlDataColumnType x) [S.TextColumn]

-- | 'S.BlobColumn'.
instance DecodeDefault B.ByteString where
   decodeDefault = Decode \case
      S.SQLBlob x -> Right x
      x -> Left $ ErrDecode_Type (sqlDataColumnType x) [S.BlobColumn]

-- | 'S.NullColumn'.
instance DecodeDefault Null where
   decodeDefault = Decode \case
      S.SQLNull -> Right mempty
      x -> Left $ ErrDecode_Type (sqlDataColumnType x) [S.NullColumn]

--------------------------------------------------------------------------------
-- Extra decodes

-- | 'S.TextColumn'.
instance DecodeDefault TL.Text where
   decodeDefault = TL.fromStrict <$> decodeDefault
   {-# INLINE decodeDefault #-}

-- | 'S.TextColumn'.
instance DecodeDefault Char where
   decodeDefault = flip decodeRefine decodeDefault \t ->
      if T.length t == 1
         then Right (T.unsafeHead t)
         else Left "Expected single character string"

-- | 'S.TextColumn'.
instance DecodeDefault String where
   decodeDefault = T.unpack <$> decodeDefault
   {-# INLINE decodeDefault #-}

-- | 'S.BlobColumn'.
instance DecodeDefault BL.ByteString where
   decodeDefault = BL.fromStrict <$> decodeDefault
   {-# INLINE decodeDefault #-}

-- | See 'decodeMaybe'.
instance (DecodeDefault a) => DecodeDefault (Maybe a) where
   decodeDefault = decodeMaybe decodeDefault
   {-# INLINE decodeDefault #-}

-- | Attempt to decode @a@ first, otherwise attempt decode
-- a 'S.NullColumn' as 'Nothing'.
decodeMaybe :: Decode a -> Decode (Maybe a)
decodeMaybe da = fmap Just da <|> fmap (\_ -> Nothing) (decodeDefault @Null)
{-# INLINE decodeMaybe #-}

-- | See 'decodeEither'.
instance
   (DecodeDefault a, DecodeDefault b)
   => DecodeDefault (Either a b)
   where
   decodeDefault = decodeEither decodeDefault decodeDefault
   {-# INLINE decodeDefault #-}

-- | @
-- 'decodeEither' da db = fmap 'Left' da '<|>' fmap 'Right' db
-- @
--
-- __WARNING__ This is probably not what you are looking for. The
-- underlying 'S.SQLData' doesn't carry any /tag/ for discriminating
-- between @a@ and @b@.
decodeEither :: Decode a -> Decode b -> Decode (Either a b)
decodeEither da db = fmap Left da <|> fmap Right db
{-# INLINE decodeEither #-}

-- | See 'decodeNS'.
instance
   (SOP.All DecodeDefault xs)
   => DecodeDefault (SOP.NS SOP.I xs)
   where
   decodeDefault =
      decodeNS (SOP.hcpure (Proxy @DecodeDefault) decodeDefault)
   {-# INLINE decodeDefault #-}

-- | Like 'decodeEither', but for arbitraryly large "Data.SOP".'NS' sums.
--
-- __WARNING__ This is probably not what you are looking for. The underlying
-- 'S.SQLData' doesn't carry any /tag/ for discriminating among @xs@.
decodeNS :: SOP.NP Decode xs -> Decode (SOP.NS SOP.I xs)
decodeNS = hasum
{-# INLINE decodeNS #-}

-- | 'S.IntegerColumn', 'S.FloatColumn', 'S.TextColumn'
-- depicting a literal integer.
instance DecodeDefault Integer where
   decodeDefault = Decode \case
      S.SQLInteger i -> Right (fromIntegral i)
      S.SQLFloat d
         | not (isNaN d || isInfinite d)
         , (i, 0) <- properFraction d ->
            Right i
         | otherwise -> first ErrDecode_Fail do
            Ex.throwString "Not an integer"
      S.SQLText t
         | Just i <- readMaybe (T.unpack t) -> Right i
         | otherwise -> first ErrDecode_Fail do
            Ex.throwString "Not an integer"
      x -> Left $ ErrDecode_Type (sqlDataColumnType x) do
         [S.IntegerColumn, S.FloatColumn, S.TextColumn]

-- | 'S.IntegerColumn'.
decodeSizedIntegral :: (Integral a, Bits a) => Decode a
decodeSizedIntegral = do
   i <- decodeDefault @Integer
   case toIntegralSized i of
      Just a -> pure a
      Nothing -> fail "Integral overflow or underflow"

-- | 'S.IntegerColumn'.
instance DecodeDefault Int8 where
   decodeDefault = decodeSizedIntegral
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn'.
instance DecodeDefault Word8 where
   decodeDefault = decodeSizedIntegral
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn'.
instance DecodeDefault Int16 where
   decodeDefault = decodeSizedIntegral
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn'.
instance DecodeDefault Word16 where
   decodeDefault = decodeSizedIntegral
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn'.
instance DecodeDefault Int32 where
   decodeDefault = decodeSizedIntegral
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn'.
instance DecodeDefault Word32 where
   decodeDefault = decodeSizedIntegral
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn' if it fits in 'Int64', otherwise 'S.TextColumn'.
instance DecodeDefault Word where
   decodeDefault = decodeSizedIntegral
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn' if it fits in 'Int64', otherwise 'S.TextColumn'.
instance DecodeDefault Word64 where
   decodeDefault = decodeSizedIntegral
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn'.
instance DecodeDefault Int where
   decodeDefault =
      caseWordSize_32_64
         decodeSizedIntegral
         (fromIntegral <$> decodeDefault @Int64)
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn' if it fits in 'Int64', otherwise 'S.TextColumn'.
instance DecodeDefault Natural where
   decodeDefault = decodeSizedIntegral
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn' and 'S.FloatColumn' only.
--
-- @0@ is 'False', every other number is 'True'.
instance DecodeDefault Bool where
   decodeDefault = Decode \case
      S.SQLInteger x -> Right (x /= 0)
      S.SQLFloat x -> Right (x /= 0)
      x ->
         Left $
            ErrDecode_Type
               (sqlDataColumnType x)
               [S.IntegerColumn, S.FloatColumn]

-- | Like for 'Time.ZonedTime'.
instance DecodeDefault Time.UTCTime where
   decodeDefault = Time.zonedTimeToUTC <$> decodeDefault
   {-# INLINE decodeDefault #-}

-- | 'S.TextColumn' ('Time.ISO8601', or seconds since Epoch with optional decimal
-- part of up to picosecond precission), or 'S.Integer' (seconds since Epoch).
--
-- TODO: Currently precission over picoseconds is successfully parsed but
-- silently floored. This is an issue in "Data.Time.Format.ISO8601". Fix.
instance DecodeDefault Time.ZonedTime where
   decodeDefault = Decode \case
      S.SQLText (T.unpack -> s)
         | Just zt <- Time.iso8601ParseM s -> Right zt
         | Just u <- Time.iso8601ParseM s ->
            Right $ Time.utcToZonedTime Time.utc u
         | Just u <- Time.parseTimeM False Time.defaultTimeLocale "%s%Q" s ->
            Right $ Time.utcToZonedTime Time.utc u
         | otherwise -> first ErrDecode_Fail do
            Ex.throwString $ "Invalid timestamp: " <> show s
      S.SQLInteger i ->
         Right $
            Time.utcToZonedTime Time.utc $
               Time.posixSecondsToUTCTime $
                  fromIntegral i
      x ->
         Left $
            ErrDecode_Type
               (sqlDataColumnType x)
               [S.IntegerColumn, S.TextColumn]

-- | 'Time.ISO8601' in a @'S.TextColumn'.
instance DecodeDefault Time.LocalTime where
   decodeDefault = decodeDefault >>= Time.iso8601ParseM
   {-# INLINE decodeDefault #-}

-- | 'Time.ISO8601' in a @'S.TextColumn'.
instance DecodeDefault Time.Day where
   decodeDefault = decodeDefault >>= Time.iso8601ParseM
   {-# INLINE decodeDefault #-}

-- | 'Time.ISO8601' in a @'S.TextColumn'.
--
-- TODO: Currently precission over picoseconds is successfully parsed but
-- silently floored. This is an issue in "Data.Time.Format.ISO8601". Fix.
instance DecodeDefault Time.TimeOfDay where
   decodeDefault = decodeDefault >>= Time.iso8601ParseM
   {-# INLINE decodeDefault #-}

-- | 'Time.ISO8601' in a @'S.TextColumn'.
--
-- TODO: Currently precission over picoseconds is successfully parsed but
-- silently floored. This is an issue in "Data.Time.Format.ISO8601". Fix.
instance DecodeDefault Time.CalendarDiffDays where
   decodeDefault = decodeDefault >>= Time.iso8601ParseM
   {-# INLINE decodeDefault #-}

-- | 'Time.ISO8601' in a @'S.TextColumn'.
--
-- TODO: Currently precission over picoseconds is successfully parsed but
-- silently floored. This is an issue in "Data.Time.Format.ISO8601". Fix.
instance DecodeDefault Time.CalendarDiffTime where
   decodeDefault = decodeDefault >>= Time.iso8601ParseM
   {-# INLINE decodeDefault #-}

-- | 'Time.ISO8601' in a @'S.TextColumn'.
instance DecodeDefault Time.TimeZone where
   decodeDefault = decodeDefault >>= Time.iso8601ParseM
   {-# INLINE decodeDefault #-}

-- | 'S.FloatColumn'.
instance DecodeDefault Float where
   decodeDefault = flip decodeRefine decodeDefault \d -> do
      let f = double2Float d
      if float2Double f == d
         then Right f
         else Left "Lossy conversion from Double to Float"

--------------------------------------------------------------------------------

-- @'decodeBinary'  =  'decodeBinary'' "Data.Binary".'Bin.get'@
decodeBinary :: Bin.Binary a => Decode a
decodeBinary = decodeBinary' Bin.get
{-# INLINE decodeBinary #-}

-- | 'S.BlobColumn'.
decodeBinary' :: Bin.Get a -> Decode a
decodeBinary' ga = flip decodeRefine (decodeDefault @BL.ByteString) \bl ->
   case Bin.runGetOrFail ga bl of
      Right (_, _, a) -> Right a
      Left (_, _, s) -> Left s

-- | 'S.TextColumn'.
decodeRead :: (Prelude.Read a) => Decode a
decodeRead = decodeRefine readEither (decodeDefault @String)
{-# INLINE decodeRead #-}

-- | @'decodeAeson' = 'decodeAeson'' "Data.Aeson".'Ae.parseJSON'@
decodeAeson :: (Ae.FromJSON a) => Decode a
decodeAeson = decodeAeson' Ae.parseJSON
{-# INLINE decodeAeson #-}

-- | 'S.TextColumn'.
decodeAeson' :: (Ae.Value -> Ae.Parser a) -> Decode a
decodeAeson' p =
   decodeRefine
      (Ae.eitherDecodeStrictText >=> Ae.parseEither p)
      (decodeDefault @T.Text)
