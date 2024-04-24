module Sq.Support
   ( resourceVanishedWithCallStack
   , note
   , hushThrow
   , show'
   , newUnique
   , acquireTmpDir
   , releaseTypeException
   , manyTill1
   , foldPostmapM
   , foldList
   , foldNonEmptyM
   , foldMaybeM
   , foldZeroM
   , foldOneM
   ) where

import Control.Applicative
import Control.Exception.Safe qualified as Ex
import Control.Foldl qualified as F
import Control.Monad hiding (void)
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource.Extra qualified as R
import Data.Acquire qualified as A
import Data.Function
import Data.IORef
import Data.Int
import Data.List.NonEmpty qualified as NEL
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
{-# INLINE note #-}

hushThrow :: (Ex.Exception e, Ex.MonadThrow m) => Either e b -> m b
hushThrow = either Ex.throwM pure
{-# INLINE hushThrow #-}

show' :: forall b a. (IsString b, Show a) => a -> b
show' = fromString . show
{-# INLINE show' #-}

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

--------------------------------------------------------------------------------

foldPostmapM :: (Monad m) => (a -> m r) -> F.FoldM m x a -> F.FoldM m x r
foldPostmapM f (F.FoldM step begin done) = F.FoldM step begin (done >=> f)

foldList :: F.Fold o (Int64, [o])
foldList = (,) <$> F.genericLength <*> F.list

foldNonEmptyM
   :: (Ex.MonadThrow m, Ex.Exception e)
   => e
   -- ^ Zero.
   -> F.FoldM m o (Int64, NEL.NonEmpty o)
foldNonEmptyM e = flip foldPostmapM (F.generalize foldList) \case
   (n, os) | Just nos <- NEL.nonEmpty os -> pure (n, nos)
   _ -> Ex.throwM e

foldMaybeM
   :: (Ex.MonadThrow m, Ex.Exception e)
   => e
   -- ^ More than one.
   -> F.FoldM m o (Maybe o)
foldMaybeM e =
   F.FoldM
      (maybe (pure . Just) \_ _ -> Ex.throwM e)
      (pure Nothing)
      pure

foldZeroM
   :: (Ex.MonadThrow m, Ex.Exception e)
   => e
   -- ^ More than zero.
   -> F.FoldM m o ()
foldZeroM e = F.FoldM (\_ _ -> Ex.throwM e) (pure ()) pure

foldOneM
   :: (Ex.MonadThrow m, Ex.Exception e)
   => e
   -- ^ Zero.
   -> e
   -- ^ More than one.
   -> F.FoldM m o o
foldOneM e0 eN = foldPostmapM (maybe (Ex.throwM e0) pure) (foldMaybeM eN)
