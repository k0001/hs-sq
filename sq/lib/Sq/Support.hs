module Sq.Support
   ( resourceVanishedWithCallStack
   , note
   , show'
   , newUnique
   , acquireTmpDir
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
import GHC.IO.Exception
import GHC.Stack
import Numeric.Natural
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
--

newUnique :: (MonadIO m) => m Natural
newUnique = liftIO $ atomicModifyIORef' iorefUnique \n -> (n + 1, n)

iorefUnique :: IORef Natural
iorefUnique = unsafePerformIO (newIORef 0)
{-# NOINLINE iorefUnique #-}

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

--------------------------------------------------------------------------------

manyTill1 :: forall f z a. (MonadPlus f) => f z -> f a -> f ([a], z)
manyTill1 fz fa = go id
  where
   go :: ([a] -> [a]) -> f ([a], z)
   go !acc =
      optional fz >>= \case
         Just z | !as <- acc [] -> pure (as, z)
         Nothing -> fa >>= \ !a -> go (acc . (a :))
