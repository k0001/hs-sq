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
   , HAsum (..)
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
import Data.SOP qualified as SOP
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

--------------------------------------------------------------------------------

class HAsum h xs where
   -- | Composes products 'SOP.NP' and 'SOP.POP' using 'Applicative', and
   -- sums 'SOP.NS' and 'SOP.SOP' using 'Alternative'.
   hasum :: (Alternative f) => SOP.Prod h f xs -> f (h SOP.I xs)

instance (SOP.All SOP.Top xs) => HAsum SOP.NP xs where
   hasum = asum_NP
   {-# INLINE hasum #-}

instance HAsum SOP.NS xs where
   hasum = asum_NS
   {-# INLINE hasum #-}

instance (SOP.All2 SOP.Top xss) => HAsum SOP.POP xss where
   hasum = asum_POP
   {-# INLINE hasum #-}

instance (SOP.All2 SOP.Top xss) => HAsum SOP.SOP xss where
   hasum = asum_SOP
   {-# INLINE hasum #-}

-- | Keeps the first 'Alternative' that succeeds among @xs@.
asum_NS :: (Alternative f) => SOP.NP f xs -> f (SOP.NS SOP.I xs)
asum_NS = \case
   this SOP.:* SOP.Nil ->
      -- We handle this case specially so we can preserve 'this''s error.
      fmap (SOP.Z . SOP.I) this
   this SOP.:* rest ->
      fmap (SOP.Z . SOP.I) this <|> fmap SOP.S (asum_NS rest)
   SOP.Nil ->
      -- We could use the type system to force @xs@ to be non-empty,
      -- but this approach probably has better ergonomics for users.
      empty

-- | Keeps the first 'Alternative' that succeeds among @xss@.
asum_SOP
   :: (Alternative f, SOP.All2 SOP.Top xss)
   => SOP.POP f xss
   -> f (SOP.SOP SOP.I xss)
asum_SOP = \(SOP.POP pp) -> fmap SOP.SOP $ case pp of
   this SOP.:* SOP.Nil ->
      -- We handle this case specially so we can preserve 'this''s error.
      fmap SOP.Z (asum_NP this)
   this SOP.:* rest ->
      fmap SOP.Z (asum_NP this)
         <|> fmap (SOP.S . SOP.unSOP) (asum_SOP (SOP.POP rest))
   SOP.Nil ->
      -- We could use the type system to force @xss@ to be non-empty,
      -- but this approach probably has better ergonomics for users.
      empty

-- | This is just 'SOP.sequence_NP'.  It doesn't use any 'Alternative'
-- features.  We write it down for completeness.
asum_NP
   :: (Applicative f, SOP.All SOP.Top xs)
   => SOP.NP f xs
   -> f (SOP.NP SOP.I xs)
asum_NP = SOP.hsequence
{-# INLINE asum_NP #-}

-- | This is just 'SOP.sequence_POP'.  It doesn't use any 'Alternative'
-- features.  We write it down for completeness.
asum_POP
   :: (Applicative f, SOP.All2 SOP.Top xss)
   => SOP.POP f xss
   -> f (SOP.POP SOP.I xss)
asum_POP = SOP.hsequence
{-# INLINE asum_POP #-}
