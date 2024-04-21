{-# LANGUAGE StrictData #-}

module Sq.Output
   ( Output
   , ErrOutput (..)
   , decode
   , runOutput
   , output
   ) where

import Control.Applicative
import Control.Exception.Safe qualified as Ex
import Control.Monad
import Control.Monad.Trans.Resource qualified as R hiding (runResourceT)
import Data.String
import Database.SQLite3 qualified as S

import Sq.Decoders
import Sq.Names

--------------------------------------------------------------------------------

data Output o
   = Output_Pure o
   | Output_Fail Ex.SomeException
   | Output_Decode BindingName (Decoder (Output o))

data ErrOutput
   = ErrOutput_ColumnValue BindingName ErrDecoder
   | ErrOutput_ColumnMissing BindingName
   | ErrOutput_Fail Ex.SomeException
   deriving stock (Show)
   deriving anyclass (Ex.Exception)

decode :: Name -> Decoder o -> Output o
decode n vda = Output_Decode (bindingName n) (Output_Pure <$> vda)
{-# INLINE decode #-}

output :: Name -> Output o -> Output o
output n = \case
   Output_Decode bn d ->
      Output_Decode (bindingName n <> bn) (output n <$> d)
   o -> o

runOutput
   :: (Monad m)
   => (BindingName -> m (Maybe S.SQLData))
   -> Output o
   -> m (Either ErrOutput o)
runOutput f = \case
   Output_Decode bn vda -> do
      f bn >>= \case
         Just s -> case runDecoder vda s of
            Right d -> runOutput f d
            Left e -> pure $ Left $ ErrOutput_ColumnValue bn e
         Nothing -> pure $ Left $ ErrOutput_ColumnMissing bn
   Output_Pure a -> pure $ Right a
   Output_Fail e -> pure $ Left $ ErrOutput_Fail e

instance Functor Output where
   fmap = liftA
   {-# INLINE fmap #-}

instance Applicative Output where
   pure = Output_Pure
   {-# INLINE pure #-}
   liftA2 = liftM2
   {-# INLINE liftA2 #-}

instance Monad Output where
   Output_Decode n vda >>= k =
      Output_Decode n (fmap (>>= k) vda)
   Output_Pure a >>= k = k a
   Output_Fail e >>= _ = Output_Fail e

instance Ex.MonadThrow Output where
   throwM = Output_Fail . Ex.toException

instance MonadFail Output where
   fail = Ex.throwString

instance (Semigroup o) => Semigroup (Output o) where
   (<>) = liftA2 (<>)
   {-# INLINE (<>) #-}

instance (Monoid o) => Monoid (Output o) where
   mempty = pure mempty
   {-# INLINE mempty #-}

instance (DefaultDecoder i) => IsString (Output i) where
   fromString s = decode (fromString s) defaultDecoder
   {-# INLINE fromString #-}
