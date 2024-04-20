module Sq.Support
   ( resourceVanishedWithCallStack
   , note
   , show'
   , newUnique
   , acquireTmpDir
   , releaseTypeException
   , manyTill1
   ) where

import Control.Applicative
import Control.Exception.Safe qualified as Ex
import Control.Monad hiding (void)
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource.Extra qualified as R
import Data.Acquire qualified as A
import Data.Function
import Data.IORef
import Data.String
import Data.Word
import GHC.IO.Exception
import GHC.Stack
import System.Directory
import System.FilePath
import System.IO.Error (isAlreadyExistsError)
import System.IO.Unsafe

--------------------------------------------------------------------------------

resourceVanishedWithCallStack :: (HasCallStack) => String -> IOError
resourceVanishedWithCallStack s =
   (userError s)
      { ioe_location = prettyCallStack (popCallStack callStack)
      , ioe_type = ResourceVanished
      }

note :: a -> Maybe b -> Either a b
note a = maybe (Left a) Right

show' :: forall b a. (IsString b, Show a) => a -> b
show' = fromString . show

--------------------------------------------------------------------------------

-- | Generate a 'Word64' unique within this OS process.
--
-- If once per nanosecond, it will take 548 years to run out of unique 'Word64'
-- identifiers. Thus, we don't check whether for overflow.
newUnique :: (MonadIO m) => m Word64
newUnique = liftIO $ atomicModifyIORef' _iorefUnique \n -> (n + 1, n)

_iorefUnique :: IORef Word64
_iorefUnique = unsafePerformIO (newIORef 0)
{-# NOINLINE _iorefUnique #-}

--------------------------------------------------------------------------------

acquireTmpDir :: A.Acquire FilePath
acquireTmpDir = flip R.mkAcquire1 removeDirectoryRecursive do
   d0 <- getTemporaryDirectory
   fix $ \k -> do
      u <- newUnique
      let d1 = d0 </> "sq.tmp." <> show u
      Ex.catchJust
         (guard . isAlreadyExistsError)
         (d1 <$ createDirectory d1)
         (const k)

releaseTypeException :: A.ReleaseType -> Maybe Ex.SomeException
releaseTypeException = \case
   A.ReleaseNormal -> Nothing
   A.ReleaseEarly -> Nothing
   A.ReleaseExceptionWith e -> Just e

--------------------------------------------------------------------------------

manyTill1 :: forall f z a. (MonadPlus f) => f z -> f a -> f ([a], z)
manyTill1 fz fa = go id
  where
   go :: ([a] -> [a]) -> f ([a], z)
   go !acc =
      optional fz >>= \case
         Just z | !as <- acc [] -> pure (as, z)
         Nothing -> fa >>= \ !a -> go (acc . (a :))
