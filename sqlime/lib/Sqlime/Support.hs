module Sqlime.Support where

import Control.Concurrent.MVar
import Control.Exception.Safe qualified as Ex
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Resource.Internal qualified as R
import Data.Acquire.Internal qualified as A
import Data.IORef
import Data.IntMap.Strict qualified as IntMap
import Data.Map.Strict qualified as Map
import GHC.IO.Exception
import GHC.Stack
import Streaming qualified as Z
import Streaming.Prelude qualified as Z
import System.IO.Unsafe

--------------------------------------------------------------------------------

mkAcquire1 :: IO a -> (a -> IO ()) -> A.Acquire a
mkAcquire1 m f = A.mkAcquire m (onceK f)

mkAcquireType1 :: IO a -> (a -> A.ReleaseType -> IO ()) -> A.Acquire a
mkAcquireType1 m f = A.mkAcquireType m (curry (onceK (uncurry f)))

withSelfReleaseAcquire
   :: A.Acquire ((A.ReleaseType -> IO ()) -> x)
   -> A.Acquire x
withSelfReleaseAcquire (A.Acquire f) = A.Acquire \restore -> do
   A.Allocated g rel0 <- f restore
   let rel1 = onceK rel0
   pure $ A.Allocated (g rel1) rel1

resourceVanishedWithCallStack :: (HasCallStack) => String -> IOError
resourceVanishedWithCallStack s =
   (userError s)
      { ioe_location = prettyCallStack (popCallStack callStack)
      , ioe_type = ResourceVanished
      }

mapPop :: (Ord k) => k -> Map.Map k v -> (Maybe v, Map.Map k v)
mapPop k m0 | (ml, yv, mr) <- Map.splitLookup k m0 = (yv, ml <> mr)

-- | 'acquireReleaseKey' will 'R.unprotect' the 'R.ReleaseKey',
-- and use 'A.Acquire' to manage the release action instead.
acquireReleaseKey :: R.ReleaseKey -> A.Acquire ()
acquireReleaseKey (R.ReleaseKey istate key) =
   void $ A.mkAcquireType acq rel
  where
   acq :: IO (Maybe (A.ReleaseType -> IO ()))
   acq =
      -- The following code does pretty much the same as 'R.unprotect',
      -- which we can't use directly because its result doesn't allow us
      -- to specify the 'A.ReleaseType' during release.
      atomicModifyIORef istate \case
         R.ReleaseMap next rf im
            | Just g <- IntMap.lookup key im ->
               (R.ReleaseMap next rf (IntMap.delete key im), Just g)
         rm -> (rm, Nothing)
   rel :: Maybe (A.ReleaseType -> IO ()) -> A.ReleaseType -> IO ()
   rel = maybe mempty id

newtype Restore m = Restore (forall x. m x -> m x)

getRestoreIO :: (MonadIO m) => m (Restore IO)
getRestoreIO =
   -- Ugly, but safe. Check the implementation in base.
   liftIO $ Ex.mask \f -> pure (Restore f)

-- | Like 'withAcquireRelease', but doesn't take the extra release function.
withAcquire
   :: (Ex.MonadMask m, MonadIO m)
   => A.Acquire a
   -> (a -> m b)
   -> m b
withAcquire (A.Acquire f) g = do
   Restore restoreIO <- getRestoreIO
   Ex.mask \restoreM -> do
      A.Allocated x free <- liftIO $ f restoreIO
      b <- Ex.withException (restoreM (g x)) \e ->
         liftIO $ free $ A.ReleaseExceptionWith e
      liftIO $ free A.ReleaseNormal
      pure b

-- | @__'withAcquireRelease' acq \\rel a -> act__@ acquires the @a@ and
-- automaticaly releases it when @mb@ returns or throws an exception.
-- If desired, 'rel' can be used to release @a@ earlier.
withAcquireRelease
   :: (Ex.MonadMask m, MonadIO m)
   => A.Acquire a
   -> ((A.ReleaseType -> IO ()) -> a -> m b)
   -> m b
withAcquireRelease (A.Acquire f) g = do
   Restore restoreIO <- getRestoreIO
   Ex.mask \restoreM -> do
      A.Allocated x free <- liftIO $ f restoreIO
      -- Wrapper so that we don't perform `free` again if `g` already did.
      let free1 = onceK free
      b <- Ex.withException (restoreM (g free1 x)) \e ->
         liftIO $ free1 $ A.ReleaseExceptionWith e
      liftIO $ free1 A.ReleaseNormal
      pure b

registerType
   :: (R.MonadResource m) => (A.ReleaseType -> IO ()) -> m R.ReleaseKey
registerType = R.liftResourceT . R.ResourceT . flip R.registerType

-- | Perform the effects necessary to obtain the first @a@,
-- and then prepend said @a@ to the output 'Z.Stream'.
wiggle :: (Monad m) => Z.Stream (Z.Of a) m r -> m (Z.Stream (Z.Of a) m r)
wiggle = fmap (either pure \(a, z) -> Z.yield a >> z) . Z.next

-- | @'once' ma@ wraps @ma@ so that @ma@ is executed at most once. Further
-- executions of the same @'once' ma@ are a no-op. It's safe to use the wrapper
-- concurrently; only one thread will get to execute the actual @ma@ at most.
once :: (MonadIO m, Ex.MonadMask m) => m () -> m ()
once ma = onceK (const ma) ()

-- | Kleisli version of 'once'.
onceK :: (MonadIO m, Ex.MonadMask m) => (a -> m ()) -> (a -> m ())
{-# NOINLINE onceK #-}
onceK kma = unsafePerformIO do
   done <- newMVar False
   pure \a ->
      Ex.bracket
         (liftIO $ takeMVar done)
         (\_ -> liftIO $ putMVar done True)
         (\d -> unless d (kma a))
