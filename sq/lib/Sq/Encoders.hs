module Sq.Encoders
   ( Encode (..)
   , ErrEncode (..)
   , encodeRefine
   , EncodeDefault (..)
   , encodeMaybe
   , encodeEither
   , encodeSizedIntegral
   , encodeBinary
   , encodeShow
   )
where

import Control.Exception.Safe qualified as Ex
import Data.Bifunctor
import Data.Binary.Put qualified as Bin
import Data.Bits
import Data.Bool
import Data.ByteString qualified as B
import Data.ByteString.Builder qualified as BB
import Data.ByteString.Lazy qualified as BL
import Data.ByteString.Short qualified as BS
import Data.Coerce
import Data.Functor.Contravariant
import Data.Int
import Data.List qualified as List
import Data.Text qualified as T
import Data.Text.Lazy qualified as TL
import Data.Text.Lazy.Builder qualified as TB
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
     -- this function to fail with 'ErrEncoder' if necessary, in which case an
     -- 'ErrInput' exception will be eventually thrown while trying to bind the
     -- relevant 'Input' to a 'Statement'. For example, not all 'String's can be
     -- safely encoded as a 'S.SQLText' because some non-unicode characters
     -- will silently be lost in the conversion. So, we either don't have an
     -- 'Encode'r for 'String' at all, which is annoying, or we have 'ErrEncode'
     -- here to safely deal with those obscure corner cases.
     Encode (a -> Either ErrEncode S.SQLData)
   deriving (Contravariant) via Op (Either ErrEncode S.SQLData)

unEncode :: Encode a -> a -> Either ErrEncode S.SQLData
unEncode = coerce
{-# INLINE unEncode #-}

-- | See v'Encode'.
newtype ErrEncode = ErrEncode Ex.SomeException
   deriving stock (Show)
   deriving anyclass (Ex.Exception)

--------------------------------------------------------------------------------

-- | Default way to encode a Haskell value of type @a@ into a SQLite value.
--
-- If there there exist a 'Sq.DecodeDefault' value for @a@, then these two
-- instances must roundtrip.
class EncodeDefault a where
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

encodeEither :: Encode a -> Encode b -> Encode (Either a b)
encodeEither (Encode fa) (Encode fb) = Encode $ either fa fb
{-# INLINE encodeEither #-}

-- | 'S.IntegerColumn'. Encodes 'False' as @0@ and 'True' as @1@.
instance EncodeDefault Bool where
   encodeDefault = bool 0 1 >$< encodeDefault @Int64
   {-# INLINE encodeDefault #-}

instance EncodeDefault Float where
   encodeDefault = float2Double >$< encodeDefault
   {-# INLINE encodeDefault #-}

instance EncodeDefault Int8 where
   encodeDefault = fromIntegral >$< encodeDefault @Int64
   {-# INLINE encodeDefault #-}

instance EncodeDefault Word8 where
   encodeDefault = fromIntegral >$< encodeDefault @Int64
   {-# INLINE encodeDefault #-}

instance EncodeDefault Int16 where
   encodeDefault = fromIntegral >$< encodeDefault @Int64
   {-# INLINE encodeDefault #-}

instance EncodeDefault Word16 where
   encodeDefault = fromIntegral >$< encodeDefault @Int64
   {-# INLINE encodeDefault #-}

instance EncodeDefault Int32 where
   encodeDefault = fromIntegral >$< encodeDefault @Int64
   {-# INLINE encodeDefault #-}

instance EncodeDefault Word32 where
   encodeDefault = fromIntegral >$< encodeDefault @Int64
   {-# INLINE encodeDefault #-}

instance EncodeDefault Int where
   encodeDefault = fromIntegral >$< encodeDefault @Int64
   {-# INLINE encodeDefault #-}

instance EncodeDefault Word where
   encodeDefault = encodeSizedIntegral
   {-# INLINE encodeDefault #-}

instance EncodeDefault Word64 where
   encodeDefault = encodeSizedIntegral
   {-# INLINE encodeDefault #-}

instance EncodeDefault Integer where
   encodeDefault = encodeSizedIntegral
   {-# INLINE encodeDefault #-}

instance EncodeDefault Natural where
   encodeDefault = encodeSizedIntegral
   {-# INLINE encodeDefault #-}

-- | 'S.IntegerColumn' if it fits in 'Int64', otherwise 'S.TextColumn'.
encodeSizedIntegral :: (Integral a, Bits a, HasCallStack) => Encode a
encodeSizedIntegral = Encode \a ->
   case toIntegralSized a of
      Just i -> unEncode (encodeDefault @Int64) i
      Nothing -> unEncode (encodeDefault @String) (show (toInteger a))

instance EncodeDefault TL.Text where
   encodeDefault = TL.toStrict >$< encodeDefault
   {-# INLINE encodeDefault #-}

instance EncodeDefault TB.Builder where
   encodeDefault = TB.toLazyText >$< encodeDefault
   {-# INLINE encodeDefault #-}

instance EncodeDefault Char where
   encodeDefault = pure >$< encodeDefault @String
   {-# INLINE encodeDefault #-}

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

instance EncodeDefault BL.ByteString where
   encodeDefault = BL.toStrict >$< encodeDefault
   {-# INLINE encodeDefault #-}

instance EncodeDefault BS.ShortByteString where
   encodeDefault = BS.fromShort >$< encodeDefault
   {-# INLINE encodeDefault #-}

instance EncodeDefault BB.Builder where
   encodeDefault = BB.toLazyByteString >$< encodeDefault
   {-# INLINE encodeDefault #-}

-- | ISO-8601 in a @'S.TextColumn'.
--
-- @yyyy-mm-ddThh:mm:ss/[.ssssssssssss]/+00:00@
--
-- * Sorting these lexicographically corresponds to sorting them by time.
--
-- * __WARNING__: SQLite date and time functions support resolution only up to
-- millisenconds.
--
-- * __WARNING__: SQLite date and time functions don't support leap seconds.
instance EncodeDefault Time.UTCTime where
   encodeDefault =
      contramap
         (Time.iso8601Show . Time.utcToZonedTime Time.utc)
         encodeDefault

--------------------------------------------------------------------------------

encodeBinary :: (a -> Bin.Put) -> Encode a
encodeBinary f = contramap (Bin.runPut . f) encodeDefault
{-# INLINE encodeBinary #-}

encodeShow :: (Show a) => Encode a
encodeShow = show >$< encodeDefault
{-# INLINE encodeShow #-}
