module Sq.Encoders
   ( Encoder (..)
   , runEncoder
   , ErrEncoder (..)
   , refineEncoder
   , refineEncoderString
   , DefaultEncoder (..)
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

newtype Encoder a = Encoder (a -> Either ErrEncoder S.SQLData)
   deriving (Contravariant) via Op (Either ErrEncoder S.SQLData)

runEncoder :: Encoder a -> a -> Either ErrEncoder S.SQLData
runEncoder = coerce
{-# INLINE runEncoder #-}

newtype ErrEncoder = ErrEncoder Ex.SomeException
   deriving stock (Show)
   deriving anyclass (Ex.Exception)

--------------------------------------------------------------------------------

class DefaultEncoder a where
   defaultEncoder :: (HasCallStack) => Encoder a

refineEncoderString
   :: (HasCallStack) => (s -> Either String a) -> Encoder a -> Encoder s
refineEncoderString f = refineEncoder \s ->
   case f s of
      Right a -> Right a
      Left e -> first ErrEncoder (Ex.throwString e)

refineEncoder :: (s -> Either ErrEncoder a) -> Encoder a -> Encoder s
refineEncoder f (Encoder g) = Encoder (f >=> g)
{-# INLINE refineEncoder #-}

--------------------------------------------------------------------------------
-- Core encoders

-- | Literal 'S.SQLData' 'Encoder'.
instance DefaultEncoder S.SQLData where
   defaultEncoder = Encoder Right
   {-# INLINE defaultEncoder #-}


-- | 'S.TextColumn'.
instance DefaultEncoder T.Text where
   defaultEncoder = S.SQLText >$< defaultEncoder
   {-# INLINE defaultEncoder #-}

{- TODO  avoid encoding '\000' ?

   defaultEncoder = Encoder \t ->
      case T.elem '\000' t of
         False -> Right $ S.SQLText t
         True ->
            first ErrEncoder $
               Ex.throwString ("Invalid character " <> show '\000' <> " in " <> show t)
-}

-- | 'S.IntegerColumn'.
instance DefaultEncoder Int64 where
   defaultEncoder = S.SQLInteger >$< defaultEncoder
   {-# INLINE defaultEncoder #-}

-- | 'S.FloatColumn'.
instance DefaultEncoder Double where
   defaultEncoder = S.SQLFloat >$< defaultEncoder
   {-# INLINE defaultEncoder #-}

-- | 'S.BlobColumn'.
instance DefaultEncoder B.ByteString where
   defaultEncoder = S.SQLBlob >$< defaultEncoder
   {-# INLINE defaultEncoder #-}

-- | 'S.NullColumn'.
instance DefaultEncoder Null where
   defaultEncoder = const S.SQLNull >$< defaultEncoder
   {-# INLINE defaultEncoder #-}

--------------------------------------------------------------------------------
-- Extra encoders

instance DefaultEncoder Void where
   defaultEncoder = Encoder absurd

-- | See 'encodeMaybe'.
instance (DefaultEncoder a) => DefaultEncoder (Maybe a) where
   defaultEncoder = encodeMaybe defaultEncoder
   {-# INLINE defaultEncoder #-}

-- | @a@'s 'S.ColumnType' if 'Just', otherwise 'S.NullColumn'.
encodeMaybe :: Encoder a -> Encoder (Maybe a)
encodeMaybe (Encoder f) = Encoder $ maybe (Right S.SQLNull) f
{-# INLINE encodeMaybe #-}

-- | See 'encodeEither'.
instance
   (DefaultEncoder a, DefaultEncoder b)
   => DefaultEncoder (Either a b)
   where
   defaultEncoder = encodeEither defaultEncoder defaultEncoder

encodeEither :: Encoder a -> Encoder b -> Encoder (Either a b)
encodeEither (Encoder fa) (Encoder fb) = Encoder $ either fa fb
{-# INLINE encodeEither #-}

-- | 'S.IntegerColumn'. Encodes 'False' as @0@ and 'True' as @1@.
instance DefaultEncoder Bool where
   defaultEncoder = bool 0 1 >$< defaultEncoder @Int64
   {-# INLINE defaultEncoder #-}

instance DefaultEncoder Float where
   defaultEncoder = float2Double >$< defaultEncoder
   {-# INLINE defaultEncoder #-}

instance DefaultEncoder Int8 where
   defaultEncoder = fromIntegral >$< defaultEncoder @Int64
   {-# INLINE defaultEncoder #-}

instance DefaultEncoder Word8 where
   defaultEncoder = fromIntegral >$< defaultEncoder @Int64
   {-# INLINE defaultEncoder #-}

instance DefaultEncoder Int16 where
   defaultEncoder = fromIntegral >$< defaultEncoder @Int64
   {-# INLINE defaultEncoder #-}

instance DefaultEncoder Word16 where
   defaultEncoder = fromIntegral >$< defaultEncoder @Int64
   {-# INLINE defaultEncoder #-}

instance DefaultEncoder Int32 where
   defaultEncoder = fromIntegral >$< defaultEncoder @Int64
   {-# INLINE defaultEncoder #-}

instance DefaultEncoder Word32 where
   defaultEncoder = fromIntegral >$< defaultEncoder @Int64
   {-# INLINE defaultEncoder #-}

instance DefaultEncoder Int where
   defaultEncoder = fromIntegral >$< defaultEncoder @Int64
   {-# INLINE defaultEncoder #-}

instance DefaultEncoder Word where
   defaultEncoder = encodeSizedIntegral
   {-# INLINE defaultEncoder #-}

instance DefaultEncoder Word64 where
   defaultEncoder = encodeSizedIntegral
   {-# INLINE defaultEncoder #-}

instance DefaultEncoder Integer where
   defaultEncoder = encodeSizedIntegral
   {-# INLINE defaultEncoder #-}

instance DefaultEncoder Natural where
   defaultEncoder = encodeSizedIntegral
   {-# INLINE defaultEncoder #-}

-- | 'S.IntegerColumn' if it fits in 'Int64', otherwise 'S.TextColumn'.
encodeSizedIntegral :: (Integral a, Bits a, HasCallStack) => Encoder a
encodeSizedIntegral = Encoder \a ->
   case toIntegralSized a of
      Just i -> runEncoder (defaultEncoder @Int64) i
      Nothing -> runEncoder (defaultEncoder @String) (show (toInteger a))

instance DefaultEncoder TL.Text where
   defaultEncoder = TL.toStrict >$< defaultEncoder
   {-# INLINE defaultEncoder #-}

instance DefaultEncoder TB.Builder where
   defaultEncoder = TB.toLazyText >$< defaultEncoder
   {-# INLINE defaultEncoder #-}

instance DefaultEncoder Char where
   defaultEncoder = pure >$< defaultEncoder @String
   {-# INLINE defaultEncoder #-}

instance DefaultEncoder String where
   defaultEncoder = Encoder \xc ->
      case List.find invalid xc of
         Nothing -> runEncoder defaultEncoder (T.pack xc)
         Just c ->
            first ErrEncoder $
               Ex.throwString ("Invalid character " <> show c)
     where
      invalid :: Char -> Bool
      invalid = \c -> '\55296' <= c && c <= '\57343'

instance DefaultEncoder BL.ByteString where
   defaultEncoder = BL.toStrict >$< defaultEncoder
   {-# INLINE defaultEncoder #-}

instance DefaultEncoder BS.ShortByteString where
   defaultEncoder = BS.fromShort >$< defaultEncoder
   {-# INLINE defaultEncoder #-}

instance DefaultEncoder BB.Builder where
   defaultEncoder = BB.toLazyByteString >$< defaultEncoder
   {-# INLINE defaultEncoder #-}

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
instance DefaultEncoder Time.UTCTime where
   defaultEncoder =
      contramap
         (Time.iso8601Show . Time.utcToZonedTime Time.utc)
         defaultEncoder

--------------------------------------------------------------------------------

encodeBinary :: (a -> Bin.Put) -> Encoder a
encodeBinary f = contramap (Bin.runPut . f) defaultEncoder
{-# INLINE encodeBinary #-}

encodeShow :: (Show a) => Encoder a
encodeShow = show >$< defaultEncoder
{-# INLINE encodeShow #-}
