module Sq.Decoders
   ( Decode (..)
   , ErrDecode (..)
   , decodeRefine
   , decodeReffineString
   , DefaultDecode (..)
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

newtype Decode a = Decode (S.SQLData -> Either ErrDecode a)
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

data ErrDecode
   = -- | Got, expected.
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

decodeReffineString
   :: (HasCallStack) => (a -> Either String b) -> Decode a -> Decode b
decodeReffineString f = decodeRefine \a ->
   case f a of
      Right b -> Right b
      Left s -> first ErrDecode_Fail (Ex.throwString s)

decodeRefine :: (a -> Either ErrDecode b) -> Decode a -> Decode b
decodeRefine f (Decode g) = Decode (g >=> f)
{-# INLINE decodeRefine #-}

--------------------------------------------------------------------------------
-- Core decodes

class DefaultDecode a where
   defaultDecode :: Decode a

-- | Literal 'S.SQLData' 'Decode'.
instance DefaultDecode S.SQLData where
   defaultDecode = Decode Right
   {-# INLINE defaultDecode #-}

instance DefaultDecode Int64 where
   defaultDecode = Decode \case
      S.SQLInteger x -> Right x
      x -> Left $ ErrDecode_Type (sqlDataColumnType x) [S.IntegerColumn]

instance DefaultDecode Double where
   defaultDecode = Decode \case
      S.SQLFloat x -> Right x
      x -> Left $ ErrDecode_Type (sqlDataColumnType x) [S.FloatColumn]

instance DefaultDecode T.Text where
   defaultDecode = Decode \case
      S.SQLText x -> Right x
      x -> Left $ ErrDecode_Type (sqlDataColumnType x) [S.TextColumn]

instance DefaultDecode B.ByteString where
   defaultDecode = Decode \case
      S.SQLBlob x -> Right x
      x -> Left $ ErrDecode_Type (sqlDataColumnType x) [S.BlobColumn]

instance DefaultDecode Null where
   defaultDecode = Decode \case
      S.SQLNull -> Right mempty
      x -> Left $ ErrDecode_Type (sqlDataColumnType x) [S.NullColumn]

--------------------------------------------------------------------------------
-- Extra decodes

instance DefaultDecode TL.Text where
   defaultDecode = TL.fromStrict <$> defaultDecode
   {-# INLINE defaultDecode #-}

instance DefaultDecode Char where
   defaultDecode = flip decodeReffineString defaultDecode \t ->
      if T.length t == 1
         then Right (T.unsafeHead t)
         else Left "Expected single character string"

instance DefaultDecode String where
   defaultDecode = T.unpack <$> defaultDecode
   {-# INLINE defaultDecode #-}

instance DefaultDecode BL.ByteString where
   defaultDecode = BL.fromStrict <$> defaultDecode
   {-# INLINE defaultDecode #-}

-- | See 'decodeMaybe'.
instance (DefaultDecode a) => DefaultDecode (Maybe a) where
   defaultDecode = decodeMaybe defaultDecode
   {-# INLINE defaultDecode #-}

-- | Attempt to decode @a@ first, otherwise decode a 'S.NullColumn'
-- as 'Notthing'.
decodeMaybe :: Decode a -> Decode (Maybe a)
decodeMaybe da = fmap Just da <|> fmap (\_ -> Nothing) (defaultDecode @Null)
{-# INLINE decodeMaybe #-}

-- | See 'decodeEither'.
instance
   (DefaultDecode a, DefaultDecode b)
   => DefaultDecode (Either a b)
   where
   defaultDecode = decodeEither defaultDecode defaultDecode
   {-# INLINE defaultDecode #-}

-- | Attempt to decode @a@ first, otherwise attempt to decode @b@, otherwise
-- fail with @b@'s parsing error.
--
-- @
-- 'decodeEither' da db = fmap 'Left' da '<|>' fmap 'Right' db
-- @
decodeEither :: Decode a -> Decode b -> Decode (Either a b)
decodeEither da db = fmap Left da <|> fmap Right db
{-# INLINE decodeEither #-}

-- | 'S.IntegerColumn', 'S.FloatColumn', 'S.TextColumn'
-- depicting a literal integer.
instance DefaultDecode Integer where
   defaultDecode = Decode \case
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
   i <- defaultDecode @Integer
   case toIntegralSized i of
      Just a -> pure a
      Nothing -> fail "Integral overflow or underflow"

instance DefaultDecode Int8 where
   defaultDecode = decodeSizedIntegral
   {-# INLINE defaultDecode #-}

instance DefaultDecode Word8 where
   defaultDecode = decodeSizedIntegral
   {-# INLINE defaultDecode #-}

instance DefaultDecode Int16 where
   defaultDecode = decodeSizedIntegral
   {-# INLINE defaultDecode #-}

instance DefaultDecode Word16 where
   defaultDecode = decodeSizedIntegral
   {-# INLINE defaultDecode #-}

instance DefaultDecode Int32 where
   defaultDecode = decodeSizedIntegral
   {-# INLINE defaultDecode #-}

instance DefaultDecode Word32 where
   defaultDecode = decodeSizedIntegral
   {-# INLINE defaultDecode #-}

instance DefaultDecode Word where
   defaultDecode = decodeSizedIntegral
   {-# INLINE defaultDecode #-}

instance DefaultDecode Word64 where
   defaultDecode = decodeSizedIntegral
   {-# INLINE defaultDecode #-}

instance DefaultDecode Int where
   defaultDecode =
      caseWordSize_32_64
         decodeSizedIntegral
         (fromIntegral <$> defaultDecode @Int64)
   {-# INLINE defaultDecode #-}

instance DefaultDecode Natural where
   defaultDecode = decodeSizedIntegral
   {-# INLINE defaultDecode #-}

-- 'S.IntegerColumn' and 'S.FloatColumn' only.
instance DefaultDecode Bool where
   defaultDecode = Decode \case
      S.SQLInteger x -> Right (x /= 0)
      S.SQLFloat x -> Right (x /= 0)
      x ->
         Left $
            ErrDecode_Type
               (sqlDataColumnType x)
               [S.IntegerColumn, S.FloatColumn]

-- | Like for 'Time.ZonedTime'.
instance DefaultDecode Time.UTCTime where
   defaultDecode = Time.zonedTimeToUTC <$> defaultDecode
   {-# INLINE defaultDecode #-}

-- 'S.TextColumn' (ISO8601, or seconds since Epoch with optional decimal
-- part of up to picosecond precission), or 'S.Integer' (seconds since Epoch
-- with optional decimal part of up to picosencond precission).
--
-- TODO: Currently precission over picoseconds is successfully parsed but
-- silently floored. Fix parser, and make it faster too.
instance DefaultDecode Time.ZonedTime where
   defaultDecode = Decode \case
      S.SQLText (T.unpack -> s)
         | Just zt <- Time.iso8601ParseM s -> Right zt
         | Just u <- Time.iso8601ParseM s ->
            Right $ Time.utcToZonedTime Time.utc u
         | Just u <- Time.parseTimeM False Time.defaultTimeLocale "%s%Q" s ->
            Right $ Time.utcToZonedTime Time.utc u
         | otherwise ->
            first ErrDecode_Fail $ Ex.throwString $ "Invalid timestamp format: " <> show s
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

instance DefaultDecode Float where
   defaultDecode = flip decodeReffineString defaultDecode \d -> do
      let f = double2Float d
      if float2Double f == d
         then Right f
         else Left "Lossy conversion from Double to Float"

--------------------------------------------------------------------------------

decodeBinary :: Bin.Get a -> Decode a
decodeBinary ga = flip decodeReffineString defaultDecode \bl ->
   case Bin.runGetOrFail ga bl of
      Right (_, _, a) -> Right a
      Left (_, _, s) -> Left s

decodeRead :: (Prelude.Read a) => Decode a
decodeRead = decodeReffineString readEither defaultDecode
{-# INLINE decodeRead #-}
