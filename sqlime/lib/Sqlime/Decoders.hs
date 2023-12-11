module Sqlime.Decoders
   ( refineDecoder
   , DefaultDecoder (..)
   , decodeMaybe
   , decodeEither
   , decodeSizedIntegral
   ) where

import Control.Applicative
import Control.Monad
import Data.Bifunctor
import Data.Bits
import Data.ByteString qualified as B
import Data.ByteString.Builder.Prim.Internal (caseWordSize_32_64)
import Data.Int
import Data.List.NonEmpty qualified as NEL
import Data.Text qualified as T
import Data.Word
import Database.SQLite3 qualified as S
import Text.Read (readMaybe)

import Sqlime.Internal

--------------------------------------------------------------------------------

refineDecoder :: (a -> Either String b) -> Decoder a -> Decoder b
refineDecoder f da = Decoder (runDecoder da >=> first ErrDecoder_Fail . f)

--------------------------------------------------------------------------------
-- Core decoders

class DefaultDecoder a where
   defaultDecoder :: Decoder a

-- | Literal 'S.SQLData' 'Decoder'.
instance DefaultDecoder S.SQLData where
   defaultDecoder = Decoder Right

instance DefaultDecoder Int64 where
   defaultDecoder = Decoder \case
      S.SQLInteger x -> Right x
      _ -> Left $ ErrDecoder_Type $ pure S.IntegerColumn

instance DefaultDecoder Double where
   defaultDecoder = Decoder \case
      S.SQLFloat x -> Right x
      _ -> Left $ ErrDecoder_Type $ pure S.FloatColumn

instance DefaultDecoder T.Text where
   defaultDecoder = Decoder \case
      S.SQLText x -> Right x
      _ -> Left $ ErrDecoder_Type $ pure S.TextColumn

instance DefaultDecoder B.ByteString where
   defaultDecoder = Decoder \case
      S.SQLBlob x -> Right x
      _ -> Left $ ErrDecoder_Type $ pure S.BlobColumn

instance DefaultDecoder Null where
   defaultDecoder = Decoder \case
      S.SQLNull -> Right Null
      _ -> Left $ ErrDecoder_Type $ pure S.NullColumn

--------------------------------------------------------------------------------
-- Extra decoders

-- | See 'decodeMaybe'.
instance (DefaultDecoder a) => DefaultDecoder (Maybe a) where
   defaultDecoder = decodeMaybe defaultDecoder

-- | Attempt to decode @a@ first, otherwise decode a 'S.NullColumn'
-- as 'Notthing'.
decodeMaybe :: Decoder a -> Decoder (Maybe a)
decodeMaybe da = fmap Just da <|> fmap (\Null -> Nothing) defaultDecoder

-- | See 'decodeEither'.
instance
   (DefaultDecoder a, DefaultDecoder b)
   => DefaultDecoder (Either a b)
   where
   defaultDecoder = decodeEither defaultDecoder defaultDecoder

-- | Attempt to decode @a@ first, otherwise attempt to decode @b@, otherwise
-- fail with @b@'s parsing error.
--
-- @
-- 'decodeEither' da db = fmap 'Left' da '<|>' fmap 'Right' db
-- @
decodeEither :: Decoder a -> Decoder b -> Decoder (Either a b)
decodeEither da db = fmap Left da <|> fmap Right db

-- | 'S.IntegerColumn', 'S.FloatColumn', 'S.TextColumn'
-- depicting a literal integer.
instance DefaultDecoder Integer where
   defaultDecoder = Decoder \case
      S.SQLInteger i -> Right (fromIntegral i)
      S.SQLFloat d
         | not (isNaN d || isInfinite d)
         , (i, 0) <- properFraction d ->
            Right i
         | otherwise -> Left $ ErrDecoder_Fail "Not an integer"
      S.SQLText t
         | Just i <- readMaybe (T.unpack t) -> Right i
         | otherwise -> Left $ ErrDecoder_Fail "Not an integer"
      _ ->
         Left $
            ErrDecoder_Type $
               NEL.fromList
                  [S.IntegerColumn, S.FloatColumn, S.TextColumn]

-- | 'S.IntegerColumn'.
decodeSizedIntegral :: (Integral a, Bits a) => Decoder a
decodeSizedIntegral =
   refineDecoder
      (maybe (Left "Integral overflow or underflow") Right . toIntegralSized)
      (defaultDecoder @Integer)

instance DefaultDecoder Int8 where
   defaultDecoder = decodeSizedIntegral
instance DefaultDecoder Word8 where
   defaultDecoder = decodeSizedIntegral
instance DefaultDecoder Int16 where
   defaultDecoder = decodeSizedIntegral
instance DefaultDecoder Word16 where
   defaultDecoder = decodeSizedIntegral
instance DefaultDecoder Int32 where
   defaultDecoder = decodeSizedIntegral
instance DefaultDecoder Word32 where
   defaultDecoder = decodeSizedIntegral
instance DefaultDecoder Word where
   defaultDecoder = decodeSizedIntegral
instance DefaultDecoder Word64 where
   defaultDecoder = decodeSizedIntegral
instance DefaultDecoder Int where
   defaultDecoder =
      caseWordSize_32_64
         decodeSizedIntegral
         (fromIntegral <$> defaultDecoder @Int64)
