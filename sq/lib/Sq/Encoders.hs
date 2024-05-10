module Sq.Encoders
   ( Encode (..)
   , ErrEncode (..)
   , encodeRefine
   , EncodeDefault (..)
   , encodeMaybe
   , encodeEither
   , encodeNS
   , encodeSizedIntegral
   , encodeBinary
   , encodeBinary'
   , encodeShow
   , encodeAeson
   , encodeAeson'
   )
where

import Control.Exception.Safe qualified as Ex
import Data.Aeson qualified as Ae
import Data.Bifunctor
import Data.Binary qualified as Bin
import Data.Binary.Put qualified as Bin
import Data.Bits
import Data.Bool
import Data.ByteString qualified as B
import Data.ByteString.Builder qualified as BB
import Data.ByteString.Lazy qualified as BL
import Data.ByteString.Short qualified as BS
import Data.Coerce
import Data.Functor.Contravariant
import Data.Functor.Contravariant.Rep
import Data.Int
import Data.List qualified as List
import Data.Profunctor
import Data.Proxy
import Data.SOP qualified as SOP
import Data.Text qualified as T
import Data.Text.Lazy qualified as TL
import Data.Text.Lazy.Builder qualified as TB
import Data.Text.Lazy.Encoding qualified as TL
import Data.Time qualified as Time
import Data.Time.Format.ISO8601 qualified as Time
import Data.Void
import Data.Word
import Database.SQLite3 qualified as S
import GHC.Float (float2Double)
import GHC.Stack
import Numeric.Natural

import Sq.Null (Null)

--------------------------------------------------------------------------------

-- | How to encode a single Haskell value of type @a@ into a SQLite value.
newtype Encode a
   = -- | Encode a value of type @a@ as 'S.SQLData'.
     --
     -- Ideally, the type @a@ should be small enough that this function always
     -- returns 'Right'. However, that can sometimes be annoying, so we allow
     -- this function to fail with 'ErrEncode' if necessary, in which case an
     -- 'Sq.ErrInput' exception will be eventually thrown while trying to bind the
     -- relevant 'Sq.Input' to a 'Statement'. Why? Because for example, not all
     -- 'String's can be safely encoded as a 'S.SQLText' seeing as some
     -- non-unicode characters will silently be lost in the conversion. So, we
     -- could either not have an 'Encode'r for 'String' at all, which would be
     -- annoying, or we could have 'ErrEncode' as we do here in order to safely
     -- deal with those obscure corner cases.
     Encode (a -> Either ErrEncode S.SQLData)
   deriving (Contravariant) via Op (Either ErrEncode S.SQLData)

instance Representable Encode where
   type Rep Encode = Either ErrEncode S.SQLData
   tabulate = Encode
   index = unEncode

unEncode :: Encode a -> a -> Either ErrEncode S.SQLData
unEncode = coerce
{-# INLINE unEncode #-}

-- | See v'Encode'.
newtype ErrEncode = ErrEncode Ex.SomeException
   deriving stock (Show)
   deriving anyclass (Ex.Exception)

--------------------------------------------------------------------------------

-- | Default way to encode a Haskell value of type @a@ into a single
-- SQLite column value.
--
-- If there there exist also a 'Sq.DecodeDefault' instance for @a@, then it
-- must roundtrip with the 'Sq.EncodeDefault' instance for @a@.
class EncodeDefault a where
   -- | Default way to encode a Haskell value of type @a@ into a single
   -- SQLite column value.
   encodeDefault :: (HasCallStack) => Encode a

-- | A convenience function for refining an 'Encode'r through a function that
-- may fail with a 'String' error message. The 'CallStack' is preserved.
--
-- If you need a more sophisticated refinement, use the 'Encode' constructor.
encodeRefine
   :: (HasCallStack)
   => (a -> Either String b)
   -> Encode b
   -> Encode a
encodeRefine f (Encode g) = Encode \a ->
   case f a of
      Right b -> g b
      Left e -> first ErrEncode (Ex.throwString e)

--------------------------------------------------------------------------------
-- Core encodes

-- | Literal 'S.SQLData' 'Encode'.
instance EncodeDefault S.SQLData where
   encodeDefault = Encode Right
   {-# INLINE encodeDefault #-}

-- | 'S.TextColumn'.
instance EncodeDefault T.Text where
   encodeDefault = S.SQLText >$< encodeDefault
   {-# INLINE encodeDefault #-}

{- TODO  avoid encoding '\000' ?

   encodeDefault = Encode \t ->
      case T.elem '\000' t of
         False -> Right $ S.SQLText t
         True ->
            first ErrEncode $
               Ex.throwString ("Invalid character " <> show '\000' <> " in " <> show t)
-}

-- | 'S.IntegerColumn'.
instance EncodeDefault Int64 where
   encodeDefault = S.SQLInteger >$< encodeDefault
   {-# INLINE encodeDefault #-}

-- | 'S.FloatColumn'.
instance EncodeDefault Double where
   encodeDefault = S.SQLFloat >$< encodeDefault
   {-# INLINE encodeDefault #-}

-- | 'S.BlobColumn'.
instance EncodeDefault B.ByteString where
   encodeDefault = S.SQLBlob >$< encodeDefault
   {-# INLINE encodeDefault #-}

-- | 'S.NullColumn'.
instance EncodeDefault Null where
   encodeDefault = const S.SQLNull >$< encodeDefault
   {-# INLINE encodeDefault #-}

--------------------------------------------------------------------------------
-- Extra encodes

-- | This is 'absurd'.
instance EncodeDefault Void where
   encodeDefault = Encode absurd

-- | See 'encodeMaybe'.
instance (EncodeDefault a) => EncodeDefault (Maybe a) where
   encodeDefault = encodeMaybe encodeDefault
   {-# INLINE encodeDefault #-}

-- | @a@'s 'S.ColumnType' if 'Just', otherwise 'S.NullColumn'.
encodeMaybe :: Encode a -> Encode (Maybe a)
encodeMaybe (Encode f) = Encode $ maybe (Right S.SQLNull) f
{-# INLINE encodeMaybe #-}

-- | See 'encodeEither'.
instance
   (EncodeDefault a, EncodeDefault b)
   => EncodeDefault (Either a b)
   where
   encodeDefault = encodeEither encodeDefault encodeDefault

-- | @a@'s 'S.ColumnType' if 'Left', otherwise @b@'s 'S.ColumnType'.
--
-- __WARNING__ This is probably not what you are looking for. The
-- underlying 'S.SQLData' won't carry any /tag/ for discriminating
-- between @a@ and @b@.
encodeEither :: Encode a -> Encode b -> Encode (Either a b)
encodeEither (Encode fa) (Encode fb) = Encode $ either fa fb
{-# INLINE encodeEither #-}

-- | See 'encodeNS'.
instance
   (SOP.All EncodeDefault xs)
   => EncodeDefault (SOP.NS SOP.I xs)
   where
   encodeDefault =
      encodeNS (SOP.hcpure (Proxy @EncodeDefault) encodeDefault)
   {-# INLINE encodeDefault #-}

-- | Like 'encodeEither', but for arbitraryly large "Data.SOP".'NS' sums.
--
-- __WARNING__ This is probably not what you are looking for. The underlying
-- 'S.SQLData' won't carry any /tag/ for discriminating among @xs@.
encodeNS :: (SOP.SListI xs) => SOP.NP Encode xs -> Encode (SOP.NS SOP.I xs)
encodeNS (nse :: SOP.NP Encode xs) = Encode (SOP.hcollapse . g)
  where
   g :: SOP.NS SOP.I xs -> SOP.NS (SOP.K (Rep Encode)) xs
   g = SOP.hap (SOP.hmap f nse)
   f :: Encode a -> (SOP.I SOP.-.-> SOP.K (Rep Encode)) a
   f = SOP.fn . dimap SOP.unI SOP.K . unEncode

-- | 'S.IntegerColumn'. Encodes 'False' as @0@ and 'True' as @1@.
instance EncodeDefault Bool where
   encodeDefault = bool 0 1 >$< encodeDefault @Int64
   {-# INLINE encodeDefault #-}

-- | 'S.FloatColumn'.
instance EncodeDefault Float where
   encodeDefault = float2Double >$< encodeDefault
   {-# INLINE encodeDefault #-}

-- | 'S.IntegerColumn'.
instance EncodeDefault Int8 where
   encodeDefault = fromIntegral >$< encodeDefault @Int64
   {-# INLINE encodeDefault #-}

-- | 'S.IntegerColumn'.
instance EncodeDefault Word8 where
   encodeDefault = fromIntegral >$< encodeDefault @Int64
   {-# INLINE encodeDefault #-}

-- | 'S.IntegerColumn'.
instance EncodeDefault Int16 where
   encodeDefault = fromIntegral >$< encodeDefault @Int64
   {-# INLINE encodeDefault #-}

-- | 'S.IntegerColumn'.
instance EncodeDefault Word16 where
   encodeDefault = fromIntegral >$< encodeDefault @Int64
   {-# INLINE encodeDefault #-}

-- | 'S.IntegerColumn'.
instance EncodeDefault Int32 where
   encodeDefault = fromIntegral >$< encodeDefault @Int64
   {-# INLINE encodeDefault #-}

-- | 'S.IntegerColumn'.
instance EncodeDefault Word32 where
   encodeDefault = fromIntegral >$< encodeDefault @Int64
   {-# INLINE encodeDefault #-}

-- | 'S.IntegerColumn'.
instance EncodeDefault Int where
   encodeDefault = fromIntegral >$< encodeDefault @Int64
   {-# INLINE encodeDefault #-}

-- | 'S.IntegerColumn' if it fits in 'Int64', otherwise 'S.TextColumn'.
instance EncodeDefault Word where
   encodeDefault = encodeSizedIntegral
   {-# INLINE encodeDefault #-}

-- | 'S.IntegerColumn' if it fits in 'Int64', otherwise 'S.TextColumn'.
instance EncodeDefault Word64 where
   encodeDefault = encodeSizedIntegral
   {-# INLINE encodeDefault #-}

-- | 'S.IntegerColumn' if it fits in 'Int64', otherwise 'S.TextColumn'.
instance EncodeDefault Integer where
   encodeDefault = encodeSizedIntegral
   {-# INLINE encodeDefault #-}

-- | 'S.IntegerColumn' if it fits in 'Int64', otherwise 'S.TextColumn'.
instance EncodeDefault Natural where
   encodeDefault = encodeSizedIntegral
   {-# INLINE encodeDefault #-}

-- | 'S.IntegerColumn' if it fits in 'Int64', otherwise 'S.TextColumn'.
encodeSizedIntegral :: (Integral a, Bits a, HasCallStack) => Encode a
encodeSizedIntegral = Encode \a ->
   case toIntegralSized a of
      Just i -> unEncode (encodeDefault @Int64) i
      Nothing -> unEncode (encodeDefault @String) (show (toInteger a))

-- | 'S.TextColumn'.
instance EncodeDefault TL.Text where
   encodeDefault = TL.toStrict >$< encodeDefault
   {-# INLINE encodeDefault #-}

-- | 'S.TextColumn'.
instance EncodeDefault TB.Builder where
   encodeDefault = TB.toLazyText >$< encodeDefault
   {-# INLINE encodeDefault #-}

-- | 'S.TextColumn'.
instance EncodeDefault Char where
   encodeDefault = pure >$< encodeDefault @String
   {-# INLINE encodeDefault #-}

-- | 'S.TextColumn'.
instance EncodeDefault String where
   encodeDefault = Encode \xc ->
      case List.find invalid xc of
         Nothing -> unEncode encodeDefault (T.pack xc)
         Just c ->
            first ErrEncode $
               Ex.throwString ("Invalid character " <> show c)
     where
      invalid :: Char -> Bool
      invalid = \c -> '\55296' <= c && c <= '\57343'

-- | 'S.BlobColumn'.
instance EncodeDefault BL.ByteString where
   encodeDefault = BL.toStrict >$< encodeDefault
   {-# INLINE encodeDefault #-}

-- | 'S.BlobColumn'.
instance EncodeDefault BS.ShortByteString where
   encodeDefault = BS.fromShort >$< encodeDefault
   {-# INLINE encodeDefault #-}

-- | 'S.BlobColumn'.
instance EncodeDefault BB.Builder where
   encodeDefault = BB.toLazyByteString >$< encodeDefault
   {-# INLINE encodeDefault #-}

-- | 'Time.ISO8601' in a @'S.TextColumn'.
--
-- @yyyy-mm-ddThh:mm:ss/[.ssssssssssss]/+00:00@
--
-- * Sorting these lexicographically in SQL corresponds to sorting them by time.
--
-- * __WARNING__: SQLite date and time functions support resolution only up to
-- milliseconds.
--
-- * __WARNING__: SQLite date and time functions don't support leap seconds.
instance EncodeDefault Time.UTCTime where
   encodeDefault = Time.utcToZonedTime Time.utc >$< encodeDefault

-- | 'Time.ISO8601' in a @'S.TextColumn'.
--
-- @yyyy-mm-ddThh:mm:ss/[.ssssssssssss]/±hh:mm@
--
-- * __WARNING__: Sorting these lexicographically in SQL won't work unless the
-- offset is always the same! Convert to 'Time.UTCTime' first.
--
-- * __WARNING__: SQLite date and time functions support resolution only up to
-- milliseconds.
--
-- * __WARNING__: SQLite date and time functions don't support leap seconds.
instance EncodeDefault Time.ZonedTime where
   encodeDefault = Time.iso8601Show >$< encodeDefault

-- | 'Time.ISO8601' in a @'S.TextColumn'.
--
-- @yyyy-mm-ddThh:mm:ss/[.ssssssssssss]/@
--
-- * Sorting these lexicographically in SQL corresponds to sorting them by time.
--
-- * __WARNING__: SQLite date and time functions support resolution only up to
-- milliseconds.
--
-- * __WARNING__: SQLite date and time functions don't support leap seconds.
instance EncodeDefault Time.LocalTime where
   encodeDefault = Time.iso8601Show >$< encodeDefault

-- | ISO-8601 in a @'S.TextColumn'.
--
-- @yyyy-mm-dd@
--
-- * Sorting these lexicographically in SQL corresponds to sorting them by time.
instance EncodeDefault Time.Day where
   encodeDefault = Time.iso8601Show >$< encodeDefault

-- | 'Time.ISO8601' in a @'S.TextColumn'.
--
-- @hh:mm:ss/[.ssssssssssss]/@
--
-- * Sorting these lexicographically in SQL corresponds to sorting them by time.
--
-- * __WARNING__: SQLite date and time functions support resolution only up to
-- milliseconds.
--
-- * __WARNING__: SQLite date and time functions don't support leap seconds.
instance EncodeDefault Time.TimeOfDay where
   encodeDefault = Time.iso8601Show >$< encodeDefault

-- | 'Time.ISO8601' in a @'S.TextColumn'.
--
-- @PyYmMdD@
instance EncodeDefault Time.CalendarDiffDays where
   encodeDefault = Time.iso8601Show >$< encodeDefault

-- | 'Time.ISO8601' in a @'S.TextColumn'.
--
-- @PyYmMdDThHmMs/[.ssssssssssss]/S@
--
-- * __WARNING__: SQLite date and time functions support resolution only up to
-- milliseconds.
instance EncodeDefault Time.CalendarDiffTime where
   encodeDefault = Time.iso8601Show >$< encodeDefault

-- | 'Time.ISO8601' in a @'S.TextColumn'.
--
-- @±hh:mm@
instance EncodeDefault Time.TimeZone where
   encodeDefault = Time.iso8601Show >$< encodeDefault

-- | See @'encodeAeson' 'Left'@.
instance EncodeDefault Ae.Encoding where
   encodeDefault = encodeAeson' Left
   {-# INLINE encodeDefault #-}

-- | See @'encodeAeson' 'Right'@.
instance EncodeDefault Ae.Value where
   encodeDefault = encodeAeson' Right
   {-# INLINE encodeDefault #-}

instance EncodeDefault Bin.Put where
   encodeDefault = encodeBinary' id
   {-# INLINE encodeDefault #-}

--------------------------------------------------------------------------------

-- | @'encodeBinary'  =  'encodeBinary'' "Data.Binary".'Bin.put'
encodeBinary :: (Bin.Binary a) => Encode a
encodeBinary = encodeBinary' Bin.put
{-# INLINE encodeBinary #-}

-- | 'S.BlobColumn'.
encodeBinary' :: (a -> Bin.Put) -> Encode a
encodeBinary' f = contramap (Bin.runPut . f) (encodeDefault @BL.ByteString)
{-# INLINE encodeBinary' #-}

-- | 'S.TextColumn'.
encodeShow :: (Show a) => Encode a
encodeShow = show >$< (encodeDefault @String)
{-# INLINE encodeShow #-}

-- | @'encodeAeson'  =  'encodeAeson' ('Left' . "Data.Aeson".'Ae.toEncoding')@
encodeAeson :: (Ae.ToJSON a) => Encode a
encodeAeson = encodeAeson' (Left . Ae.toEncoding)
{-# INLINE encodeAeson #-}

-- | Encodes as 'S.TextColumn'.
encodeAeson' :: (a -> Either Ae.Encoding Ae.Value) -> Encode a
encodeAeson' f =
   contramap
      (either g (g . Ae.toEncoding) . f)
      (encodeDefault @TL.Text)
  where
   g :: Ae.Encoding -> TL.Text
   g = TL.decodeUtf8 . BB.toLazyByteString . Ae.fromEncoding
