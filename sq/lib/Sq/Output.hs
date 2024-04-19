{-# LANGUAGE StrictData #-}

module Sq.Output
   ( Output
   , ErrOutput (..)
   , decode
   , decodeWith
   , runOutput
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
   | Output_Decode Name (Decoder (Output o))

data ErrOutput
   = ErrOutput_ColumnValue Name ErrDecoder
   | ErrOutput_ColumnMissing Name
   | ErrOutput_Fail Ex.SomeException
   deriving stock (Show)
   deriving anyclass (Ex.Exception)

decode :: (DefaultDecoder o) => Name -> Output o
decode n = decodeWith n defaultDecoder
{-# INLINE decode #-}

decodeWith :: Name -> Decoder o -> Output o
decodeWith n vda = Output_Decode n (Output_Pure <$> vda)
{-# INLINE decodeWith #-}

runOutput
   :: (Monad m)
   => (Name -> m (Maybe S.SQLData))
   -> Output o
   -> m (Either ErrOutput o)
runOutput f = \case
   Output_Decode n vda -> do
      f n >>= \case
         Just s -> case runDecoder vda s of
            Right d -> runOutput f d
            Left e -> pure $ Left $ ErrOutput_ColumnValue n e
         Nothing -> pure $ Left $ ErrOutput_ColumnMissing n
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
   fromString = decode . fromString
   {-# INLINE fromString #-}
