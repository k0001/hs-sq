module Sq.Encoders
   ( refineEncoder
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
import Data.Binary.Put qualified as Bin
import Data.Bits
import Data.Bool
import Data.ByteString qualified as B
import Data.ByteString.Builder qualified as BB
import Data.ByteString.Lazy qualified as BL
import Data.ByteString.Short qualified as BS
import Data.Functor.Contravariant
import Data.Int
import Data.Text qualified as T
import Data.Text.Lazy qualified as TL
import Data.Text.Lazy.Builder qualified as TB
import Data.Time qualified as Time
import Data.Time.Format.ISO8601 qualified as Time
import Data.Word
import Database.SQLite3 qualified as S
import GHC.Float (float2Double)
import GHC.Stack
import Numeric.Natural

import Sq.Internal

--------------------------------------------------------------------------------

refineEncoderString
   :: (HasCallStack) => (s -> Either String a) -> Encoder a -> Encoder s
refineEncoderString f = refineEncoder (either Ex.throwString pure . f)

refineEncoder :: (s -> Either Ex.SomeException a) -> Encoder a -> Encoder s
refineEncoder f ea = Encoder (f >=> runEncoder ea)

class DefaultEncoder a where
   defaultEncoder :: (HasCallStack) => Encoder a

--------------------------------------------------------------------------------
-- Core encoders

-- | Literal 'S.SQLData' 'Encoder'.
instance DefaultEncoder S.SQLData where
   defaultEncoder = Encoder Right

-- | 'S.TextColumn'.
instance DefaultEncoder T.Text where
   defaultEncoder = S.SQLText >$< defaultEncoder

-- | 'S.IntegerColumn'.
instance DefaultEncoder Int64 where
   defaultEncoder = S.SQLInteger >$< defaultEncoder

-- | 'S.FloatColumn'.
instance DefaultEncoder Double where
   defaultEncoder = S.SQLFloat >$< defaultEncoder

-- | 'S.BlobColumn'.
instance DefaultEncoder B.ByteString where
   defaultEncoder = S.SQLBlob >$< defaultEncoder

-- | 'S.NullColumn'.
instance DefaultEncoder Null where
   defaultEncoder = const S.SQLNull >$< defaultEncoder

--------------------------------------------------------------------------------
-- Extra encoders

-- | See 'encodeMaybe'.
instance (DefaultEncoder a) => DefaultEncoder (Maybe a) where
   defaultEncoder = encodeMaybe defaultEncoder

-- | @a@'s 'S.ColumnType' if 'Just', otherwise 'S.NullColumn'.
encodeMaybe :: Encoder a -> Encoder (Maybe a)
encodeMaybe ea =
   Encoder $ maybe (runEncoder defaultEncoder Null) (runEncoder ea)

-- | See 'encodeEither'.
instance
   (DefaultEncoder a, DefaultEncoder b)
   => DefaultEncoder (Either a b)
   where
   defaultEncoder = encodeEither defaultEncoder defaultEncoder

-- | @a@'s 'S.ColumnType' if 'Just', otherwise 'S.NullColumn'.
encodeEither :: Encoder a -> Encoder b -> Encoder (Either a b)
encodeEither ea eb = Encoder $ either (runEncoder ea) (runEncoder eb)

-- | 'S.IntegerColumn'. Encodes 'False' as @0@ and 'True' as @1@.
instance DefaultEncoder Bool where
   defaultEncoder = bool 0 1 >$< defaultEncoder @Int64

instance DefaultEncoder Float where
   defaultEncoder = float2Double >$< defaultEncoder

instance DefaultEncoder Int8 where
   defaultEncoder = fromIntegral >$< defaultEncoder @Int64
instance DefaultEncoder Word8 where
   defaultEncoder = fromIntegral >$< defaultEncoder @Int64
instance DefaultEncoder Int16 where
   defaultEncoder = fromIntegral >$< defaultEncoder @Int64
instance DefaultEncoder Word16 where
   defaultEncoder = fromIntegral >$< defaultEncoder @Int64
instance DefaultEncoder Int32 where
   defaultEncoder = fromIntegral >$< defaultEncoder @Int64
instance DefaultEncoder Word32 where
   defaultEncoder = fromIntegral >$< defaultEncoder @Int64
instance DefaultEncoder Int where
   defaultEncoder = fromIntegral >$< defaultEncoder @Int64
instance DefaultEncoder Word where
   defaultEncoder = encodeSizedIntegral
instance DefaultEncoder Word64 where
   defaultEncoder = encodeSizedIntegral
instance DefaultEncoder Integer where
   defaultEncoder = encodeSizedIntegral
instance DefaultEncoder Natural where
   defaultEncoder = encodeSizedIntegral

-- | 'S.IntegerColumn'.
encodeSizedIntegral :: (Integral a, Bits a, HasCallStack) => Encoder a
encodeSizedIntegral =
   refineEncoderString
      ( note "Integral overflows or underflows 64-bit signed integer"
         . toIntegralSized
      )
      (defaultEncoder @Int64)

instance DefaultEncoder Char where
   defaultEncoder = T.singleton >$< defaultEncoder
instance DefaultEncoder String where
   defaultEncoder = T.pack >$< defaultEncoder
instance DefaultEncoder TL.Text where
   defaultEncoder = TL.toStrict >$< defaultEncoder
instance DefaultEncoder TB.Builder where
   defaultEncoder = TB.toLazyText >$< defaultEncoder
instance DefaultEncoder BL.ByteString where
   defaultEncoder = BL.toStrict >$< defaultEncoder
instance DefaultEncoder BS.ShortByteString where
   defaultEncoder = BS.fromShort >$< defaultEncoder
instance DefaultEncoder BB.Builder where
   defaultEncoder = BB.toLazyByteString >$< defaultEncoder

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

encodeShow :: (Show a) => Encoder a
encodeShow = show >$< defaultEncoder
