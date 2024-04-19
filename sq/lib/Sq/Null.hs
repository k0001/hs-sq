module Sq.Null (Null (..)) where

-- | The @NULL@ SQL datatype.
data Null = Null
   deriving stock (Eq, Ord, Show)

instance Semigroup Null where
   _ <> _ = Null

instance Monoid Null where
   mempty = Null
