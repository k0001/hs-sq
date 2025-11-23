{-# LANGUAGE StrictData #-}
{-# LANGUAGE UndecidableInstances #-}

module Sq.Output
   ( Output
   , ErrOutput (..)
   , decode
   , runOutput
   , output
   , OutputDefault (..)
   , goutputDefault
   , GOutputDefault
   , houtput
   , HOutput
   ) where

import Control.Applicative
import Control.Exception.Safe qualified as Ex
import Control.Monad
import Control.Monad.Trans.Resource qualified as R hiding (runResourceT)
import Data.Coerce
import Data.Kind
import Data.List.NonEmpty qualified as NEL
import Data.Proxy
import Data.String
import Database.SQLite3 qualified as S
import GHC.Generics qualified as G
import Generics.SOP qualified as SOP
import Generics.SOP.GGP qualified as SOP

import Sq.Decoders
import Sq.Names
import Sq.Support

--------------------------------------------------------------------------------

-- | How to decode an output row from a single 'Sq.Statement'.
--
-- * Construct with 'decode', 'IsString'.
--
-- * Nest with 'output'.
--
-- * Compose with 'Monoid', 'Functor', 'Applicative', 'Alternative', 'Monad',
-- 'MonadPlus', 'MonadFail' and 'Ex.MonadThrow' tools.
data Output o
   = Output_Pure o
   | Output_Fail Ex.SomeException
   | Output_Decode BindingName (Decode (Output o))

data ErrOutput
   = -- | Error from v'Decode'.
     ErrOutput_ColumnValue BindingName ErrDecode
   | -- | Missing column name in the raw 'SQL'.
     ErrOutput_ColumnMissing BindingName
   | -- | Error from 'Ex.MonadThrow'.
     ErrOutput_Fail Ex.SomeException
   deriving stock (Show)
   deriving anyclass (Ex.Exception)

-- | Decode the column with the given 'Name'.
--
-- @
-- 'Sq.readStatement'
--         'mempty'
--         ('decode' \"foo\" 'decodeDefault')
--         \"SELECT foo FROM t\"
--    :: ('DecodeDefault' x)
--    => 'Sq.Statement' 'Sq.Read' () x
-- @
--
-- Note that by design, this library doesn't support positional 'Output'
-- parameters. You must always pick a 'Name'. In the raw SQL, you can use @AS@
-- to rename your output columns as necessary.
--
-- @
-- 'Sq.readStatement'
--         'mempty'
--         ('decode' \"abc\" 'decodeDefault')
--         \"SELECT foo AS abc FROM t\"
--    :: ('DecodeDefault' x)
--    => 'Sq.Statement' 'Sq.Read' () x
-- @
--
-- Multiple 'Outputs's can be composed with 'Monoid', 'Functor', 'Applicative',
-- 'Alternative', 'Monad', 'MonadPlus', 'MonadFail' and 'Ex.MonadThrow' tools.
--
-- @
-- 'Sq.readStatement'
--         'mempty'
--         (do foo <- 'decode' \"foo\" 'decodeDefault'
--             'when' (foo > 10) do
--                'fail' \"Oh no!"
--             bar <- 'decode' \"bar\" 'decodeDefault'
--             'pure' (foo, bar))
--         \"SELECT foo, bar FROM t\"
--    :: ('DecodeDefault' y)
--    => 'Sq.Statement' 'Sq.Read' () ('Int', y)
-- @
--
-- Pro-tip: Consider using the 'IsString' instance for 'Output',
-- where for example @\"foo\"@ means @'decode' \"foo\" 'decodeDefault'@:
--
-- @
-- 'Sq.readStatement'
--         ('liftA2' (,) \"foo\" \"bar\")
--         'mempty'
--         \"SELECT foo, bar FROM t\"
--    :: ('DecodeDefault' x, 'DecodeDefault' y)
--    => 'Sq.Statement' 'Sq.Read' () (x, y)
-- @
decode :: Name -> Decode o -> Output o
decode n vda = Output_Decode (BindingName (pure n)) (Output_Pure <$> vda)
{-# INLINE decode #-}

-- | Add a prefix 'Name' to column names in the given 'Output',
-- separated by @\__@
--
-- This is useful for making reusable 'Output's. For example,
-- consider the following.
--
-- @
-- data Point = Point { x :: 'Int', y :: 'Int' }
--
-- pointOutput :: 'Output' Point
-- pointOutput = Point '<$>' \"x\" '<*>' \"y\"
-- @
--
-- After using 'output':
--
-- @
-- 'Sq.readStatement'
--         'mempty'
--         ('liftA2' ('output' \"p1\" pointOutput)
--                 ('output' \"p2\" pointOutput))
--         ['Sq.sql'|
--           SELECT ax AS p1\__x, ay AS p1\__y,
--                  bx AS p2\__x, by AS p2\__y
--           FROM vectors|]
--    :: 'Sq.Statement' 'Sq.Read' () (Point, Point)
-- @
output :: Name -> Output o -> Output o
output n = \case
   Output_Decode bn d ->
      Output_Decode (coerce (NEL.cons n) bn) (output n <$> d)
   o -> o

-- | TODO cache names after lookup. Important for Alternative.
runOutput
   :: (Monad m)
   => (BindingName -> m (Maybe S.SQLData))
   -> Output o
   -> m (Either ErrOutput o)
runOutput f = \case
   Output_Decode bn (Decode vda) -> do
      f bn >>= \case
         Just s -> case vda s of
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

instance Alternative Output where
   empty = fail "empty"
   {-# INLINE empty #-}
   l <|> r = case l of
      Output_Decode n vda ->
         Output_Decode n (fmap (<|> r) vda)
      Output_Pure _ -> l
      Output_Fail _ -> r

instance MonadPlus Output where
   mzero = fail "mzero"
   {-# INLINE mzero #-}
   mplus = (<|>)
   {-# INLINE mplus #-}

instance Monad Output where
   l >>= k = case l of
      Output_Decode n vda ->
         Output_Decode n (fmap (>>= k) vda)
      Output_Pure a -> k a
      Output_Fail e -> Output_Fail e

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

instance (DecodeDefault i) => IsString (Output i) where
   fromString s = decode (fromString s) decodeDefault
   {-# INLINE fromString #-}

--------------------------------------------------------------------------------

-- | 'Constraint' to be satisfied for using 'houtput'.
type HOutput :: ((Type -> Type) -> k -> Type) -> k -> Constraint
type HOutput = HAsum

-- | Given a "Data.SOP".'SOP.Prod' containing all the possible 'Output'
-- decoders, obtain an 'Output' for any 'SOP.NS', 'SOP.NP', 'SOP.SOP' or
-- 'SOP.POP' having that same @xs@.
--
-- Composes products 'SOP.NP' and 'SOP.POP' using 'Applicative',
-- and sums 'SOP.NS' and 'SOP.SOP' using 'Alternative'.
houtput :: (HOutput h xs) => SOP.Prod h Output xs -> Output (h SOP.I xs)
houtput = hasum
{-# INLINE houtput #-}

--------------------------------------------------------------------------------

-- | 'Constraint' to be satisfied for using 'goutputDefault'.
type GOutputDefault :: Type -> Constraint
type GOutputDefault o =
   ( OnlyRecords (SOP.GDatatypeInfoOf o)
   , SOP.All2 DecodeDefault (SOP.GCode o)
   , G.Generic o
   , SOP.GTo o
   , SOP.GDatatypeInfo o
   , HOutput SOP.SOP (SOP.GCode o)
   )

-- | Generic 'Output' implementation for types with GHC 'G.Generic' instance
-- where all the constructors are records with named fields, to be used as
-- 'Name's, and each field type has a 'DecodeDefault' instance.
--
-- If the datatype has more than one constructor, each one is tried in order
-- until one of them matches.
goutputDefault :: forall o. (GOutputDefault o) => Output o
goutputDefault =
   fmap SOP.gto $ houtput $ SOP.POP do
      case SOP.gdatatypeInfo (Proxy @o) of
         SOP.Newtype _ _ ci -> f ci SOP.:* SOP.Nil
         SOP.ADT _ _ cis _ ->
            SOP.hcmap (Proxy @(SOP.All DecodeDefault)) f cis
  where
   f
      :: forall b
       . (SOP.All DecodeDefault b)
      => SOP.ConstructorInfo b
      -> SOP.NP Output b
   f (SOP.Record _ fis) =
      SOP.hcmap
         (Proxy @DecodeDefault)
         (\(SOP.FieldInfo s) -> decode (fromString s) decodeDefault)
         fis
   f _ = undefined -- impossible due to OnlyRecords

--------------------------------------------------------------------------------

-- | Default way to decode the 'Output' from a 'Sq.Statement' as a Haskell
-- value of type @a@.
--
-- If there there exist also a 'Sq.InputDefault' instance for @o@, then it
-- must roundtrip with the 'Sq.OutputDefault' instance for @o@.
class OutputDefault o where
   outputDefault :: Output o

   -- | 'goutputDefault' is used as default implementation.
   default outputDefault :: (GOutputDefault o) => Output o
   outputDefault = goutputDefault

-- | We don't export this, because we export 'OutputDefault' instances for
-- 'SOP.NS', 'SOP.NP', 'SOP.SOP' and 'SOP.POP'.
houtputDefault
   :: ( HOutput h xs
      , SOP.AllN (SOP.Prod h) OutputDefault xs
      , SOP.HPure (SOP.Prod h)
      )
   => Output (h SOP.I xs)
houtputDefault = houtput (SOP.hcpure (Proxy @OutputDefault) outputDefault)

-- | Read "Data.SOP".
--
-- __WARNING__ This may lead to unexpected results if the underlying
-- 'OutputDefault's don't check for any /tag/ for discriminating between the
-- various @xss@.
instance
   (SOP.SListI2 xss, SOP.All2 OutputDefault xss)
   => OutputDefault (SOP.SOP SOP.I xss)
   where
   outputDefault = houtputDefault
   {-# INLINE outputDefault #-}

-- | Read "Data.SOP".
instance
   (SOP.SListI2 xss, SOP.All2 OutputDefault xss)
   => OutputDefault (SOP.POP SOP.I xss)
   where
   outputDefault = houtputDefault
   {-# INLINE outputDefault #-}

-- | Read "Data.SOP".
instance
   (SOP.SListI xs, SOP.All OutputDefault xs)
   => OutputDefault (SOP.NP SOP.I xs)
   where
   outputDefault = houtputDefault
   {-# INLINE outputDefault #-}

-- | Read "Data.SOP".
--
-- __WARNING__ This may lead to unexpected results if the underlying
-- 'OutputDefault's don't check for any /tag/ for discriminating between
-- the various @xs@.
instance
   (SOP.SListI xs, SOP.All OutputDefault xs)
   => OutputDefault (SOP.NS SOP.I xs)
   where
   outputDefault = houtputDefault
   {-# INLINE outputDefault #-}

instance (OutputDefault a, OutputDefault b) => OutputDefault (a, b) where
   outputDefault = (,) <$> outputDefault <*> outputDefault

instance
   (OutputDefault a, OutputDefault b, OutputDefault c)
   => OutputDefault (a, b, c)
   where
   outputDefault = (,,) <$> outputDefault <*> outputDefault <*> outputDefault

instance
   (OutputDefault a, OutputDefault b, OutputDefault c, OutputDefault d)
   => OutputDefault (a, b, c, d)
   where
   outputDefault =
      (,,,)
         <$> outputDefault
         <*> outputDefault
         <*> outputDefault
         <*> outputDefault

-- | __WARNING__ This may lead to unexpected results if the underlying
-- 'OutputDefault's don't check for any /tag/ for discriminating between @a@
-- and @b@.
instance (OutputDefault a, OutputDefault b) => OutputDefault (Either a b) where
   outputDefault = fmap Left outputDefault <|> fmap Right outputDefault
