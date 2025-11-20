module Sq.Decoders
   ( Decode (..)
   , ErrDecode (..)
   , decodeRefine
   , DecodeDefault (..)
   , decodeMaybe
   , decodeEither
   , decodeNS
   , decodeBoundedIntegral
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
import Data.Aeson.Parser qualified as Aep
import Data.Aeson.Types qualified as Ae
import Data.Attoparsec.ByteString qualified as AB
import Data.Attoparsec.Text qualified as AT
import Data.Attoparsec.Time qualified as AT8601
import Data.Bifunctor
import Data.Binary qualified as Bin
import Data.Binary.Get qualified as Bin
import Data.Bits
import Data.ByteString qualified as B
import Data.ByteString.Builder.Prim.Internal (caseWordSize_32_64)
import Data.ByteString.Lazy qualified as BL
import Data.Fixed
import Data.Int
import Data.Proxy
import Data.SOP qualified as SOP
import Data.Scientific qualified as Sci
import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import Data.Text.Lazy qualified as TL
import Data.Text.Unsafe qualified as T
import Data.Time qualified as Time
import Data.Time.Clock.POSIX qualified as Time

import Data.Time.Format.ISO8601 qualified as Time
import Data.UUID.Types qualified as UUID
import Data.Word
import Database.SQLite3 qualified as S
import GHC.Float (double2Float, float2Double)
import GHC.Stack
import Numeric.Natural
import Text.Read (readEither)

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

errDecodeFailString :: (HasCallStack) => String -> ErrDecode
errDecodeFailString s =
   ErrDecode_Fail $ Ex.toException $ Ex.StringException s ?callStack
{-# INLINE errDecodeFailString #-}

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
   first errDecodeFailString (f a)

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
   {-# INLINE decodeDefault #-}
   decodeDefault = Decode \case
      S.SQLInteger x -> Right x
      x -> Left $ ErrDecode_Type (sqlDataColumnType x) [S.IntegerColumn]

-- | 'S.FloatColumn'.
instance DecodeDefault Double where
   {-# INLINE decodeDefault #-}
   decodeDefault = Decode \case
      S.SQLFloat x -> Right x
      x -> Left $ ErrDecode_Type (sqlDataColumnType x) [S.FloatColumn]

-- | 'S.TextColumn'.
instance DecodeDefault T.Text where
   {-# INLINE decodeDefault #-}
   decodeDefault = Decode \case
      S.SQLText x -> Right x
      x -> Left $ ErrDecode_Type (sqlDataColumnType x) [S.TextColumn]

-- | 'S.BlobColumn'.
instance DecodeDefault B.ByteString where
   {-# INLINE decodeDefault #-}
   decodeDefault = Decode \case
      S.SQLBlob x -> Right x
      x -> Left $ ErrDecode_Type (sqlDataColumnType x) [S.BlobColumn]

-- | 'S.NullColumn'.
instance DecodeDefault Null where
   {-# INLINE decodeDefault #-}
   decodeDefault = Decode \case
      S.SQLNull -> Right mempty
      x -> Left $ ErrDecode_Type (sqlDataColumnType x) [S.NullColumn]

--------------------------------------------------------------------------------
-- Extra decodes

-- | 'S.TextColumn'.
instance DecodeDefault TL.Text where
   {-# INLINE decodeDefault #-}
   decodeDefault =
      {-# SCC "decodeDefault/TextLazy" #-}
      (TL.fromStrict <$> decodeDefault)

-- | 'S.TextColumn'.
instance DecodeDefault Char where
   decodeDefault =
      {-# SCC "decodeDefault/Char" #-}
      ( flip decodeRefine decodeDefault \t ->
         if T.length t == 1
            then Right (T.unsafeHead t)
            else Left "Expected single character string"
      )

-- | 'S.TextColumn'.
instance DecodeDefault String where
   {-# INLINE decodeDefault #-}
   decodeDefault =
      {-# SCC "decodeDefault/String" #-}
      (T.unpack <$> decodeDefault)

-- | 'S.BlobColumn'.
instance DecodeDefault BL.ByteString where
   {-# INLINE decodeDefault #-}
   decodeDefault =
      {-# SCC "decodeDefault/ByteStringLazy" #-}
      (BL.fromStrict <$> decodeDefault)

-- | See 'decodeMaybe'.
instance (DecodeDefault a) => DecodeDefault (Maybe a) where
   decodeDefault = decodeMaybe decodeDefault
   {-# INLINE decodeDefault #-}

-- | Attempt to decode @a@ first, or a 'S.NullColumn' as 'Nothing' otherwise.
decodeMaybe :: Decode a -> Decode (Maybe a)
decodeMaybe (Decode fa) = Decode \d -> case fa d of
   Right a -> Right (Just a)
   Left e -> case d of
      S.SQLNull -> Right Nothing
      _ -> Left e

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

-- | See 'decodeNS'.
instance
   (SOP.All DecodeDefault xs)
   => DecodeDefault (SOP.NS SOP.I xs)
   where
   {-# INLINE decodeDefault #-}
   decodeDefault =
      {-# SCC "decodeDefault/SOP.NS" #-}
      decodeNS (SOP.hcpure (Proxy @DecodeDefault) decodeDefault)

-- | Like 'decodeEither', but for arbitraryly large "Data.SOP".'NS' sums.
--
-- __WARNING__ This is probably not what you are looking for. The underlying
-- 'S.SQLData' doesn't carry any /tag/ for discriminating among @xs@.
decodeNS :: SOP.NP Decode xs -> Decode (SOP.NS SOP.I xs)
decodeNS = hasum

-- | 'S.IntegerColumn', 'S.FloatColumn', 'S.TextColumn'
-- depicting a literal integer.
instance DecodeDefault Integer where
   decodeDefault =
      {-# SCC "decodeDefault/Integer" #-}
      ( Decode \case
         S.SQLInteger i -> Right (fromIntegral i)
         S.SQLText t -> case scientificFromText t of
            Just s -> case Sci.floatingOrInteger s of
               Right ~i
                  | sciIntegerMin <= s && s <= sciIntegerMax -> Right i
                  | otherwise -> Left $ errDecodeFailString "Integer too large"
               Left (_ :: Double) -> Left $ errDecodeFailString "Not an integer"
            Nothing -> Left $ errDecodeFailString "Malformed number"
         S.SQLFloat d
            | isNaN d -> Left $ errDecodeFailString "NaN"
            | isInfinite d -> Left $ errDecodeFailString "Infinity"
            | (i, 0) <- properFraction d, d == fromIntegral i -> Right i
            | otherwise -> Left $ errDecodeFailString "Lossy conversion"
         x -> Left $ ErrDecode_Type (sqlDataColumnType x) do
            [S.IntegerColumn, S.FloatColumn, S.TextColumn]
      )

-- | Some probably large enough numbers.
sciIntegerMax :: Sci.Scientific
sciIntegerMax = Sci.scientific 1 (fromIntegral (maxBound :: Int16))

-- | Some probably large enough numbers.
sciIntegerMin :: Sci.Scientific
sciIntegerMin = Sci.scientific (-1) (fromIntegral (maxBound :: Int16))

-- | 'S.IntegerColumn', 'S.FloatColumn', 'S.TextColumn'.
decodeBoundedIntegral :: forall a. (Integral a, Bounded a, Bits a) => Decode a
decodeBoundedIntegral = Decode \case
   S.SQLInteger i -> f i
   S.SQLText t -> case scientificFromText t of
      Just s
         | Sci.isInteger s -> case Sci.toBoundedInteger s of
            Just a -> Right a
            Nothing -> Left $ errDecodeFailString "Overflow or underflow"
         | otherwise -> Left $ errDecodeFailString "Not an integer"
      Nothing -> Left $ errDecodeFailString "Malformed number"
   S.SQLFloat d
      | isNaN d -> Left $ errDecodeFailString "NaN"
      | isInfinite d -> Left $ errDecodeFailString "Infinity"
      | (i :: Integer, 0) <- properFraction d ->
         case f i of
            Right a
               | d == fromIntegral a -> Right a
               | otherwise -> Left $ errDecodeFailString "Lossy conversion"
            Left e -> Left e
      | otherwise -> Left $ errDecodeFailString "Not an integer"
   x -> Left $ ErrDecode_Type (sqlDataColumnType x) do
      [S.IntegerColumn, S.FloatColumn, S.TextColumn]
  where
   f :: (Bits i, Integral i) => i -> Either ErrDecode a
   f i = case toIntegralSized i of
      Just a -> Right a
      Nothing -> Left $ errDecodeFailString "Overflow or underflow"

-- | 'S.IntegerColumn'.
instance DecodeDefault Int8 where
   decodeDefault = decodeBoundedIntegral
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn'.
instance DecodeDefault Word8 where
   decodeDefault = decodeBoundedIntegral
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn'.
instance DecodeDefault Int16 where
   decodeDefault = decodeBoundedIntegral
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn'.
instance DecodeDefault Word16 where
   decodeDefault = decodeBoundedIntegral
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn'.
instance DecodeDefault Int32 where
   decodeDefault = decodeBoundedIntegral
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn'.
instance DecodeDefault Word32 where
   decodeDefault = decodeBoundedIntegral
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn' if it fits in 'Int64', otherwise 'S.TextColumn'.
instance DecodeDefault Word where
   decodeDefault = decodeBoundedIntegral
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn' if it fits in 'Int64', otherwise 'S.TextColumn'.
instance DecodeDefault Word64 where
   decodeDefault = decodeBoundedIntegral
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn'.
instance DecodeDefault Int where
   decodeDefault =
      caseWordSize_32_64
         decodeBoundedIntegral
         (fromIntegral <$> decodeDefault @Int64)
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn' if it fits in 'Int64', otherwise 'S.TextColumn'.
instance DecodeDefault Natural where
   decodeDefault =
      decodeRefine
         (maybe (Left "Underflow") Right . toIntegralSized)
         (decodeDefault @Integer)
   {-# INLINE decodeDefault #-}

-- | 'S.IntegerColumn' and 'S.FloatColumn' only.
--
-- @0@ is 'False', every other number is 'True'.
instance DecodeDefault Bool where
   decodeDefault =
      {-# SCC "decodeDefault/Bool" #-}
      ( Decode \case
         S.SQLInteger x -> Right (x /= 0)
         S.SQLFloat x -> Right (x /= 0)
         x ->
            Left $
               ErrDecode_Type
                  (sqlDataColumnType x)
                  [S.IntegerColumn, S.FloatColumn]
      )

-- | Like for 'Time.ZonedTime'.
instance DecodeDefault Time.UTCTime where
   {-# INLINE decodeDefault #-}
   decodeDefault =
      {-# SCC "decodeDefault/UTCTime" #-}
      (Time.zonedTimeToUTC <$> decodeDefault)

-- | 'S.TextColumn' ('Time.ISO8601', or seconds since Epoch with optional decimal
-- part of up to picosecond precission), or 'S.Integer' (seconds since Epoch).
--
-- TODO: Currently precission over picoseconds is successfully parsed but
-- silently floored. This is an issue in "Data.Time.Format.ISO8601". Fix.
instance DecodeDefault Time.ZonedTime where
   decodeDefault =
      {-# SCC "decodeDefault/ZonedTime" #-}
      ( Decode \case
         S.SQLText t -> case AT.parseOnly pzt t of
            Right zt -> Right zt
            Left e -> first ErrDecode_Fail do
               Ex.throwString $ "Invalid timestamp: " <> show e
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
      )
     where
      pzt =
         mplus
            ({-# SCC zonedTime #-} AT8601.zonedTime <* AT.endOfInput)
            ({-# SCC psecs #-} psecs <* AT.endOfInput)
      psecs = do
         s <- AT.scientific
         let pico :: Pico = MkFixed $ floor (s * 1_000_000_000_000)
             ndt :: Time.NominalDiffTime = Time.secondsToNominalDiffTime pico
         pure $ Time.utcToZonedTime Time.utc $ Time.posixSecondsToUTCTime ndt

-- | 'Time.ISO8601' in a @'S.TextColumn'.
instance DecodeDefault Time.LocalTime where
   {-# INLINE decodeDefault #-}
   decodeDefault =
      {-# SCC "decodeDefault/LocalTime" #-}
      ( decodeRefine
         (AT.parseOnly (AT8601.localTime <* AT.endOfInput))
         decodeDefault
      )

-- | 'Time.ISO8601' in a @'S.TextColumn'.
instance DecodeDefault Time.Day where
   {-# INLINE decodeDefault #-}
   decodeDefault =
      {-# SCC "decodeDefault/Day" #-}
      ( decodeRefine
         (AT.parseOnly (AT8601.day <* AT.endOfInput))
         decodeDefault
      )

-- | 'Time.ISO8601' in a @'S.TextColumn'.
--
-- TODO: Currently precission over picoseconds is successfully parsed but
-- silently floored. This is an issue in "Data.Time.Format.ISO8601". Fix.
instance DecodeDefault Time.TimeOfDay where
   {-# INLINE decodeDefault #-}
   decodeDefault =
      {-# SCC "decodeDefault/TimeOfDay" #-}
      ( decodeRefine
         (AT.parseOnly (AT8601.timeOfDay <* AT.endOfInput))
         decodeDefault
      )

-- | 'Time.ISO8601' in a @'S.TextColumn'.
--
-- TODO: Currently precission over picoseconds is successfully parsed but
-- silently floored. This is an issue in "Data.Time.Format.ISO8601". Fix.
instance DecodeDefault Time.CalendarDiffDays where
   {-# INLINE decodeDefault #-}
   decodeDefault =
      {-# SCC "decodeDefault/CalendarDiffDays" #-}
      (decodeDefault >>= Time.iso8601ParseM)

-- | 'Time.ISO8601' in a @'S.TextColumn'.
--
-- TODO: Currently precission over picoseconds is successfully parsed but
-- silently floored. This is an issue in "Data.Time.Format.ISO8601". Fix.
instance DecodeDefault Time.CalendarDiffTime where
   {-# INLINE decodeDefault #-}
   decodeDefault =
      {-# SCC "decodeDefault/CalendarDiffTime" #-}
      (decodeDefault >>= Time.iso8601ParseM)

-- | 'Time.ISO8601' in a @'S.TextColumn'.
instance DecodeDefault Time.TimeZone where
   {-# INLINE decodeDefault #-}
   decodeDefault =
      {-# SCC "decodeDefault/TimeZone" #-}
      ( flip decodeRefine decodeDefault \t ->
         case AT.parseOnly (AT8601.timeZone <* AT.endOfInput) t of
            Right (Just tz) -> Right tz
            Right Nothing -> Left "Not a valid time zone"
            Left s -> Left ("Not a valid time zone: " <> show s)
      )

-- | 'S.FloatColumn'.
instance DecodeDefault Float where
   decodeDefault =
      {-# SCC "decodeDefault/Float" #-}
      ( flip decodeRefine decodeDefault \d -> do
         let f = double2Float d
         if float2Double f == d
            then Right f
            else Left "Lossy conversion"
      )

-- | 'S.TextColumn'.
instance DecodeDefault UUID.UUID where
   decodeDefault =
      {-# SCC "decodeDefault/UUID" #-}
      ( decodeRefine
         (maybe (Left "Not valid UUID") Right . UUID.fromText)
         decodeDefault
      )

-- | 'S.TextColumn'.
instance DecodeDefault Ae.Value where
   decodeDefault =
      {-# SCC "decodeDefault/Ae.Value" #-}
      (decodeRefine Ae.eitherDecodeStrictText decodeDefault)

-- | 'S.IntegerColumn', 'S.FloatColumn', 'S.TextColumn'.
instance forall e. (HasResolution e) => DecodeDefault (Fixed e) where
   decodeDefault =
      {-# SCC "decodeDefault/Fixed" #-}
      ( decodeRefine
         ( \s0 ->
            let s1 = s0 * smult
            in  case Sci.floatingOrInteger s1 of
                  Right ~i
                     | sciIntegerMin <= s1 && s1 <= sciIntegerMax ->
                        Right (MkFixed i)
                     | otherwise -> Left "Integer too large"
                  Left (_ :: Double) -> Left "Lossy conversion"
         )
         decodeDefault
      )
     where
      smult :: Sci.Scientific
      smult = fromInteger (resolution (Proxy @e))

-- | 'S.IntegerColumn', 'S.FloatColumn', 'S.TextColumn'.
instance DecodeDefault Sci.Scientific where
   decodeDefault =
      {-# SCC "decodeDefault/Scientific" #-}
      ( Decode \case
         S.SQLInteger i -> Right (fromIntegral i)
         S.SQLText t -> case scientificFromText t of
            Just x -> Right x
            Nothing -> Left $ errDecodeFailString "Malformed number"
         S.SQLFloat d
            | isNaN d -> Left $ errDecodeFailString "NaN"
            | isInfinite d -> Left $ errDecodeFailString "Infinity"
            | x <- Sci.fromFloatDigits d
            , d == Sci.toRealFloat x ->
               Right x
            | otherwise ->
               Left $ errDecodeFailString "Lossy conversion"
         c ->
            Left $ ErrDecode_Type (sqlDataColumnType c) do
               [S.IntegerColumn, S.FloatColumn, S.TextColumn]
      )

scientificFromText :: T.Text -> Maybe Sci.Scientific
scientificFromText =
   either (const Nothing) Just . AB.parseOnly Aep.scientific . T.encodeUtf8

--------------------------------------------------------------------------------

-- @'decodeBinary'  =  'decodeBinary'' "Data.Binary".'Bin.get'@
decodeBinary :: (Bin.Binary a) => Decode a
decodeBinary = decodeBinary' Bin.get

-- | 'S.BlobColumn'.
decodeBinary' :: Bin.Get a -> Decode a
decodeBinary' ga = flip decodeRefine (decodeDefault @BL.ByteString) \bl ->
   case Bin.runGetOrFail ga bl of
      Right (_, _, a) -> Right a
      Left (_, _, s) -> Left s

-- | 'S.TextColumn'.
decodeRead :: (Prelude.Read a) => Decode a
decodeRead = decodeRefine readEither (decodeDefault @String)

-- | @'decodeAeson' = 'decodeAeson'' "Data.Aeson".'Ae.parseJSON'@
decodeAeson :: (Ae.FromJSON a) => Decode a
decodeAeson = decodeAeson' Ae.parseJSON

-- | 'S.TextColumn'.
decodeAeson' :: (Ae.Value -> Ae.Parser a) -> Decode a
decodeAeson' p = decodeRefine (Ae.parseEither p) (decodeDefault @Ae.Value)
