{-# LANGUAGE StrictData #-}

module Sq.Input
   ( Input
   , runInput
   , Sq.Input.absurd
   , encode
   , encodeWith
   , pushInput
   , BoundInput
   , bindInput
   , ErrInput (..)
   , runBoundInput
   ) where

import Control.DeepSeq
import Control.Exception.Safe qualified as Ex
import Control.Monad
import Data.Coerce
import Data.Functor.Contravariant
import Data.Functor.Contravariant.Divisible
import Data.Map.Strict qualified as Map
import Data.String
import Data.Text qualified as T
import Data.Void
import Database.SQLite3 qualified as S

import Sq.Encoders
import Sq.Names

--------------------------------------------------------------------------------

newtype Input i = Input (i -> Map.Map BindingName (Either ErrEncoder S.SQLData))
   deriving newtype
      ( Semigroup
        -- ^ Left-biased.
      , Monoid
      , NFData
      )
   deriving
      (Contravariant, Divisible, Decidable)
      via Op (Map.Map BindingName (Either ErrEncoder S.SQLData))

absurd :: Input Void
absurd = Input Data.Void.absurd

runInput :: Input i -> i -> Map.Map BindingName (Either ErrEncoder S.SQLData)
runInput = coerce
{-# INLINE runInput #-}

encode :: (DefaultEncoder i) => Name -> Input i
encode n = encodeWith n defaultEncoder
{-# INLINE encode #-}

encodeWith :: Name -> Encoder i -> Input i
encodeWith n e = Input (Map.singleton (bindingName n) . runEncoder e)
{-# INLINE encodeWith #-}

pushInput :: Name -> Input i -> Input i
pushInput n ba = Input \s ->
   Map.mapKeysMonotonic (bindingName n <>) (runInput ba s)
{-# INLINE pushInput #-}

instance (DefaultEncoder i) => IsString (Input i) where
   fromString = encode . fromString
   {-# INLINE fromString #-}

--------------------------------------------------------------------------------

newtype BoundInput = BoundInput [(T.Text, S.SQLData)]
   deriving newtype (Eq, Show)

bindInput :: Input i -> i -> Either ErrInput BoundInput
bindInput ii i = fmap BoundInput do
   forM (Map.toAscList (runInput ii i)) \(bn, ev) -> do
      let !k = renderInputBindingName bn
      case ev of
         Right !d -> Right (k, d)
         Left e -> Left $ ErrInput bn e

data ErrInput = ErrInput BindingName ErrEncoder
   deriving stock (Show)
   deriving anyclass (Ex.Exception)

runBoundInput :: BoundInput -> [(T.Text, S.SQLData)]
runBoundInput = coerce
{-# INLINE runBoundInput #-}
