module Sq.Null (Null (..)) where

-- | The @NULL@ SQL datatype.
--
-- Mostly useful for its 'Sq.EncodeDefault' and 'Sq.DecodeDefault'
-- instances.
data Null = Null
   deriving stock (Eq, Ord, Show)

instance Semigroup Null where
   _ <> _ = Null

instance Monoid Null where
   mempty = Null
