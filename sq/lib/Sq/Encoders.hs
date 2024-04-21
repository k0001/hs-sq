module Sq.Encoders
   ( Encode (..)
   , runEncode
   , ErrEncode (..)
   , encodeRefine
   , encodeRefineString
   , DefaultEncode (..)
   , encodeMaybe
   , encodeEither
   , encodeSizedIntegral
   , encodeBinary
   , encodeShow
   )
where

import Control.Exception.Safe qualified as Ex
import Control.Monad
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

newtype Encode a = Encode (a -> Either ErrEncode S.SQLData)
   deriving (Contravariant) via Op (Either ErrEncode S.SQLData)

runEncode :: Encode a -> a -> Either ErrEncode S.SQLData
runEncode = coerce
{-# INLINE runEncode #-}

newtype ErrEncode = ErrEncode Ex.SomeException
   deriving stock (Show)
   deriving anyclass (Ex.Exception)

--------------------------------------------------------------------------------

class DefaultEncode a where
   defaultEncode :: (HasCallStack) => Encode a

encodeRefineString
   :: (HasCallStack) => (s -> Either String a) -> Encode a -> Encode s
encodeRefineString f = encodeRefine \s ->
   case f s of
      Right a -> Right a
      Left e -> first ErrEncode (Ex.throwString e)

encodeRefine :: (s -> Either ErrEncode a) -> Encode a -> Encode s
encodeRefine f (Encode g) = Encode (f >=> g)
{-# INLINE encodeRefine #-}

--------------------------------------------------------------------------------
-- Core encodes

-- | Literal 'S.SQLData' 'Encode'.
instance DefaultEncode S.SQLData where
   defaultEncode = Encode Right
   {-# INLINE defaultEncode #-}


-- | 'S.TextColumn'.
instance DefaultEncode T.Text where
   defaultEncode = S.SQLText >$< defaultEncode
   {-# INLINE defaultEncode #-}

{- TODO  avoid encoding '\000' ?

   defaultEncode = Encode \t ->
      case T.elem '\000' t of
         False -> Right $ S.SQLText t
         True ->
            first ErrEncode $
               Ex.throwString ("Invalid character " <> show '\000' <> " in " <> show t)
-}

-- | 'S.IntegerColumn'.
instance DefaultEncode Int64 where
   defaultEncode = S.SQLInteger >$< defaultEncode
   {-# INLINE defaultEncode #-}

-- | 'S.FloatColumn'.
instance DefaultEncode Double where
   defaultEncode = S.SQLFloat >$< defaultEncode
   {-# INLINE defaultEncode #-}

-- | 'S.BlobColumn'.
instance DefaultEncode B.ByteString where
   defaultEncode = S.SQLBlob >$< defaultEncode
   {-# INLINE defaultEncode #-}

-- | 'S.NullColumn'.
instance DefaultEncode Null where
   defaultEncode = const S.SQLNull >$< defaultEncode
   {-# INLINE defaultEncode #-}

--------------------------------------------------------------------------------
-- Extra encodes

instance DefaultEncode Void where
   defaultEncode = Encode absurd

-- | See 'encodeMaybe'.
instance (DefaultEncode a) => DefaultEncode (Maybe a) where
   defaultEncode = encodeMaybe defaultEncode
   {-# INLINE defaultEncode #-}

-- | @a@'s 'S.ColumnType' if 'Just', otherwise 'S.NullColumn'.
encodeMaybe :: Encode a -> Encode (Maybe a)
encodeMaybe (Encode f) = Encode $ maybe (Right S.SQLNull) f
{-# INLINE encodeMaybe #-}

-- | See 'encodeEither'.
instance
   (DefaultEncode a, DefaultEncode b)
   => DefaultEncode (Either a b)
   where
   defaultEncode = encodeEither defaultEncode defaultEncode

encodeEither :: Encode a -> Encode b -> Encode (Either a b)
encodeEither (Encode fa) (Encode fb) = Encode $ either fa fb
{-# INLINE encodeEither #-}

-- | 'S.IntegerColumn'. Encodes 'False' as @0@ and 'True' as @1@.
instance DefaultEncode Bool where
   defaultEncode = bool 0 1 >$< defaultEncode @Int64
   {-# INLINE defaultEncode #-}

instance DefaultEncode Float where
   defaultEncode = float2Double >$< defaultEncode
   {-# INLINE defaultEncode #-}

instance DefaultEncode Int8 where
   defaultEncode = fromIntegral >$< defaultEncode @Int64
   {-# INLINE defaultEncode #-}

instance DefaultEncode Word8 where
   defaultEncode = fromIntegral >$< defaultEncode @Int64
   {-# INLINE defaultEncode #-}

instance DefaultEncode Int16 where
   defaultEncode = fromIntegral >$< defaultEncode @Int64
   {-# INLINE defaultEncode #-}

instance DefaultEncode Word16 where
   defaultEncode = fromIntegral >$< defaultEncode @Int64
   {-# INLINE defaultEncode #-}

instance DefaultEncode Int32 where
   defaultEncode = fromIntegral >$< defaultEncode @Int64
   {-# INLINE defaultEncode #-}

instance DefaultEncode Word32 where
   defaultEncode = fromIntegral >$< defaultEncode @Int64
   {-# INLINE defaultEncode #-}

instance DefaultEncode Int where
   defaultEncode = fromIntegral >$< defaultEncode @Int64
   {-# INLINE defaultEncode #-}

instance DefaultEncode Word where
   defaultEncode = encodeSizedIntegral
   {-# INLINE defaultEncode #-}

instance DefaultEncode Word64 where
   defaultEncode = encodeSizedIntegral
   {-# INLINE defaultEncode #-}

instance DefaultEncode Integer where
   defaultEncode = encodeSizedIntegral
   {-# INLINE defaultEncode #-}

instance DefaultEncode Natural where
   defaultEncode = encodeSizedIntegral
   {-# INLINE defaultEncode #-}

-- | 'S.IntegerColumn' if it fits in 'Int64', otherwise 'S.TextColumn'.
encodeSizedIntegral :: (Integral a, Bits a, HasCallStack) => Encode a
encodeSizedIntegral = Encode \a ->
   case toIntegralSized a of
      Just i -> runEncode (defaultEncode @Int64) i
      Nothing -> runEncode (defaultEncode @String) (show (toInteger a))

instance DefaultEncode TL.Text where
   defaultEncode = TL.toStrict >$< defaultEncode
   {-# INLINE defaultEncode #-}

instance DefaultEncode TB.Builder where
   defaultEncode = TB.toLazyText >$< defaultEncode
   {-# INLINE defaultEncode #-}

instance DefaultEncode Char where
   defaultEncode = pure >$< defaultEncode @String
   {-# INLINE defaultEncode #-}

instance DefaultEncode String where
   defaultEncode = Encode \xc ->
      case List.find invalid xc of
         Nothing -> runEncode defaultEncode (T.pack xc)
         Just c ->
            first ErrEncode $
               Ex.throwString ("Invalid character " <> show c)
     where
      invalid :: Char -> Bool
      invalid = \c -> '\55296' <= c && c <= '\57343'

instance DefaultEncode BL.ByteString where
   defaultEncode = BL.toStrict >$< defaultEncode
   {-# INLINE defaultEncode #-}

instance DefaultEncode BS.ShortByteString where
   defaultEncode = BS.fromShort >$< defaultEncode
   {-# INLINE defaultEncode #-}

instance DefaultEncode BB.Builder where
   defaultEncode = BB.toLazyByteString >$< defaultEncode
   {-# INLINE defaultEncode #-}

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
instance DefaultEncode Time.UTCTime where
   defaultEncode =
      contramap
         (Time.iso8601Show . Time.utcToZonedTime Time.utc)
         defaultEncode

--------------------------------------------------------------------------------

encodeBinary :: (a -> Bin.Put) -> Encode a
encodeBinary f = contramap (Bin.runPut . f) defaultEncode
{-# INLINE encodeBinary #-}

encodeShow :: (Show a) => Encode a
encodeShow = show >$< defaultEncode
{-# INLINE encodeShow #-}
