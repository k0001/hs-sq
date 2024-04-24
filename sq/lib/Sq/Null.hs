module Sq.Null (Null (..)) where

-- | The @NULL@ SQL datatype.
--
-- Mostly useful if you want to encode or decode a literal @NULL@ value
-- through 'Sq.EncodeDefault' and 'Sq.DecodeDefault' instances.
--
-- However, often you can benefit from 'Sq.encodeMaybe' and 'Sq.decodeMaybe'
-- instead.
data Null = Null
   deriving stock (Eq, Ord, Show)

instance Semigroup Null where
   _ <> _ = Null

instance Monoid Null where
   mempty = Null
