module Sq.Decoders
   ( Decoder (..)
   , runDecoder
   , ErrDecoder (..)
   , refineDecoder
   , refineDecoderString
   , DefaultDecoder (..)
   , decodeMaybe
   , decodeEither
   , decodeSizedIntegral
   , decodeBinary
   , decodeRead
   ) where

import Control.Applicative
import Control.Exception.Safe qualified as Ex
import Control.Monad
import Control.Monad.Catch qualified as Ex (MonadThrow (..))
import Control.Monad.Trans.Reader
import Data.Bifunctor
import Data.Binary.Get qualified as Bin
import Data.Bits
import Data.ByteString qualified as B
import Data.ByteString.Builder.Prim.Internal (caseWordSize_32_64)
import Data.ByteString.Lazy qualified as BL
import Data.Coerce
import Data.Int
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

--------------------------------------------------------------------------------

newtype Decoder a
   = Decoder (S.SQLData -> Either ErrDecoder a)
   deriving
      (Functor, Applicative, Monad)
      via ReaderT S.SQLData (Either ErrDecoder)

runDecoder :: Decoder a -> S.SQLData -> Either ErrDecoder a
runDecoder = coerce
{-# INLINE runDecoder #-}

-- | @'mempty' = 'pure' 'mempty'@
instance (Monoid a) => Monoid (Decoder a) where
   mempty = pure mempty
   {-# INLINE mempty #-}

-- | @('<>') == 'liftA2' ('<>')@
instance (Semigroup a) => Semigroup (Decoder a) where
   (<>) = liftA2 (<>)
   {-# INLINE (<>) #-}

instance Ex.MonadThrow Decoder where
   throwM = Decoder . const . Left . ErrDecoder_Fail . Ex.toException

instance MonadFail Decoder where
   fail = Ex.throwString
   {-# INLINE fail #-}

-- | Leftmost result on success, rightmost error on failure.
instance Alternative Decoder where
   empty = fail "empty"
   {-# INLINE empty #-}
   (<|>) = mplus
   {-# INLINE (<|>) #-}

-- | Leftmost result on success, rightmost error on failure.
instance MonadPlus Decoder where
   mzero = fail "mzero"
   {-# INLINE mzero #-}
   mplus (Decoder l) (Decoder r) = Decoder \s ->
      either (\_ -> r s) pure (l s)
   {-# INLINE mplus #-}

data ErrDecoder
   = -- | Got, expected.
     ErrDecoder_Type S.ColumnType [S.ColumnType]
   | ErrDecoder_Fail Ex.SomeException
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

refineDecoderString
   :: (HasCallStack) => (a -> Either String b) -> Decoder a -> Decoder b
refineDecoderString f = refineDecoder \a ->
   case f a of
      Right b -> Right b
      Left s -> first ErrDecoder_Fail (Ex.throwString s)

refineDecoder :: (a -> Either ErrDecoder b) -> Decoder a -> Decoder b
refineDecoder f (Decoder g) = Decoder (g >=> f)
{-# INLINE refineDecoder #-}

--------------------------------------------------------------------------------
-- Core decoders

class DefaultDecoder a where
   defaultDecoder :: Decoder a

-- | Literal 'S.SQLData' 'Decoder'.
instance DefaultDecoder S.SQLData where
   defaultDecoder = Decoder Right
   {-# INLINE defaultDecoder #-}

instance DefaultDecoder Int64 where
   defaultDecoder = Decoder \case
      S.SQLInteger x -> Right x
      x -> Left $ ErrDecoder_Type (sqlDataColumnType x) [S.IntegerColumn]

instance DefaultDecoder Double where
   defaultDecoder = Decoder \case
      S.SQLFloat x -> Right x
      x -> Left $ ErrDecoder_Type (sqlDataColumnType x) [S.FloatColumn]

instance DefaultDecoder T.Text where
   defaultDecoder = Decoder \case
      S.SQLText x -> Right x
      x -> Left $ ErrDecoder_Type (sqlDataColumnType x) [S.TextColumn]

instance DefaultDecoder B.ByteString where
   defaultDecoder = Decoder \case
      S.SQLBlob x -> Right x
      x -> Left $ ErrDecoder_Type (sqlDataColumnType x) [S.BlobColumn]

instance DefaultDecoder Null where
   defaultDecoder = Decoder \case
      S.SQLNull -> Right mempty
      x -> Left $ ErrDecoder_Type (sqlDataColumnType x) [S.NullColumn]

--------------------------------------------------------------------------------
-- Extra decoders

instance DefaultDecoder TL.Text where
   defaultDecoder = TL.fromStrict <$> defaultDecoder
   {-# INLINE defaultDecoder #-}

instance DefaultDecoder Char where
   defaultDecoder = flip refineDecoderString defaultDecoder \t ->
      if T.length t == 1
         then Right (T.unsafeHead t)
         else Left "Expected single character string"

instance DefaultDecoder String where
   defaultDecoder = T.unpack <$> defaultDecoder
   {-# INLINE defaultDecoder #-}

instance DefaultDecoder BL.ByteString where
   defaultDecoder = BL.fromStrict <$> defaultDecoder
   {-# INLINE defaultDecoder #-}

-- | See 'decodeMaybe'.
instance (DefaultDecoder a) => DefaultDecoder (Maybe a) where
   defaultDecoder = decodeMaybe defaultDecoder
   {-# INLINE defaultDecoder #-}

-- | Attempt to decode @a@ first, otherwise decode a 'S.NullColumn'
-- as 'Notthing'.
decodeMaybe :: Decoder a -> Decoder (Maybe a)
decodeMaybe da = fmap Just da <|> fmap (\_ -> Nothing) (defaultDecoder @Null)
{-# INLINE decodeMaybe #-}

-- | See 'decodeEither'.
instance
   (DefaultDecoder a, DefaultDecoder b)
   => DefaultDecoder (Either a b)
   where
   defaultDecoder = decodeEither defaultDecoder defaultDecoder
   {-# INLINE defaultDecoder #-}

-- | Attempt to decode @a@ first, otherwise attempt to decode @b@, otherwise
-- fail with @b@'s parsing error.
--
-- @
-- 'decodeEither' da db = fmap 'Left' da '<|>' fmap 'Right' db
-- @
decodeEither :: Decoder a -> Decoder b -> Decoder (Either a b)
decodeEither da db = fmap Left da <|> fmap Right db
{-# INLINE decodeEither #-}

-- | 'S.IntegerColumn', 'S.FloatColumn', 'S.TextColumn'
-- depicting a literal integer.
instance DefaultDecoder Integer where
   defaultDecoder = Decoder \case
      S.SQLInteger i -> Right (fromIntegral i)
      S.SQLFloat d
         | not (isNaN d || isInfinite d)
         , (i, 0) <- properFraction d ->
            Right i
         | otherwise -> first ErrDecoder_Fail do
            Ex.throwString "Not an integer"
      S.SQLText t
         | Just i <- readMaybe (T.unpack t) -> Right i
         | otherwise -> first ErrDecoder_Fail do
            Ex.throwString "Not an integer"
      x -> Left $ ErrDecoder_Type (sqlDataColumnType x) do
         [S.IntegerColumn, S.FloatColumn, S.TextColumn]

-- | 'S.IntegerColumn'.
decodeSizedIntegral :: (Integral a, Bits a) => Decoder a
decodeSizedIntegral = do
   i <- defaultDecoder @Integer
   case toIntegralSized i of
      Just a -> pure a
      Nothing -> fail "Integral overflow or underflow"

instance DefaultDecoder Int8 where
   defaultDecoder = decodeSizedIntegral
   {-# INLINE defaultDecoder #-}

instance DefaultDecoder Word8 where
   defaultDecoder = decodeSizedIntegral
   {-# INLINE defaultDecoder #-}

instance DefaultDecoder Int16 where
   defaultDecoder = decodeSizedIntegral
   {-# INLINE defaultDecoder #-}

instance DefaultDecoder Word16 where
   defaultDecoder = decodeSizedIntegral
   {-# INLINE defaultDecoder #-}

instance DefaultDecoder Int32 where
   defaultDecoder = decodeSizedIntegral
   {-# INLINE defaultDecoder #-}

instance DefaultDecoder Word32 where
   defaultDecoder = decodeSizedIntegral
   {-# INLINE defaultDecoder #-}

instance DefaultDecoder Word where
   defaultDecoder = decodeSizedIntegral
   {-# INLINE defaultDecoder #-}

instance DefaultDecoder Word64 where
   defaultDecoder = decodeSizedIntegral
   {-# INLINE defaultDecoder #-}

instance DefaultDecoder Int where
   defaultDecoder =
      caseWordSize_32_64
         decodeSizedIntegral
         (fromIntegral <$> defaultDecoder @Int64)
   {-# INLINE defaultDecoder #-}

instance DefaultDecoder Natural where
   defaultDecoder = decodeSizedIntegral
   {-# INLINE defaultDecoder #-}

-- 'S.IntegerColumn' and 'S.FloatColumn' only.
instance DefaultDecoder Bool where
   defaultDecoder = Decoder \case
      S.SQLInteger x -> Right (x /= 0)
      S.SQLFloat x -> Right (x /= 0)
      x ->
         Left $
            ErrDecoder_Type
               (sqlDataColumnType x)
               [S.IntegerColumn, S.FloatColumn]

-- | Like for 'Time.ZonedTime'.
instance DefaultDecoder Time.UTCTime where
   defaultDecoder = Time.zonedTimeToUTC <$> defaultDecoder
   {-# INLINE defaultDecoder #-}

-- 'S.TextColumn' (ISO8601, or seconds since Epoch with optional decimal
-- part of up to picosecond precission), or 'S.Integer' (seconds since Epoch
-- with optional decimal part of up to picosencond precission).
--
-- TODO: Currently precission over picoseconds is successfully parsed but
-- silently floored. Fix parser, and make it faster too.
instance DefaultDecoder Time.ZonedTime where
   defaultDecoder = Decoder \case
      S.SQLText (T.unpack -> s)
         | Just zt <- Time.iso8601ParseM s -> Right zt
         | Just u <- Time.iso8601ParseM s ->
            Right $ Time.utcToZonedTime Time.utc u
         | Just u <- Time.parseTimeM False Time.defaultTimeLocale "%s%Q" s ->
            Right $ Time.utcToZonedTime Time.utc u
         | otherwise ->
            first ErrDecoder_Fail $ Ex.throwString $ "Invalid timestamp format: " <> show s
      S.SQLInteger i ->
         Right $
            Time.utcToZonedTime Time.utc $
               Time.posixSecondsToUTCTime $
                  fromIntegral i
      x ->
         Left $
            ErrDecoder_Type
               (sqlDataColumnType x)
               [S.IntegerColumn, S.TextColumn]

instance DefaultDecoder Float where
   defaultDecoder = flip refineDecoderString defaultDecoder \d -> do
      let f = double2Float d
      if float2Double f == d
         then Right f
         else Left "Lossy conversion from Double to Float"

--------------------------------------------------------------------------------

decodeBinary :: Bin.Get a -> Decoder a
decodeBinary ga = flip refineDecoderString defaultDecoder \bl ->
   case Bin.runGetOrFail ga bl of
      Right (_, _, a) -> Right a
      Left (_, _, s) -> Left s

decodeRead :: (Prelude.Read a) => Decoder a
decodeRead = refineDecoderString readEither defaultDecoder
{-# INLINE decodeRead #-}
