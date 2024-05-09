{-# LANGUAGE StrictData #-}

module Sq.Output
   ( Output
   , ErrOutput (..)
   , decode
   , runOutput
   , output
   , houtput
   , HOutput
   , OutputDefault(..)
   ) where

import Control.Applicative
import Control.Exception.Safe qualified as Ex
import Control.Monad
import Control.Monad.Trans.Resource qualified as R hiding (runResourceT)
import Data.SOP qualified as SOP
import Data.String
import Database.SQLite3 qualified as S

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
decode n vda = Output_Decode (bindingName n) (Output_Pure <$> vda)
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
--         ('liftA2' ('output' \"p1\" pointInput)
--                 ('output' \"p2\" pointInput))
--         ['Sq.sql'|
--           SELECT ax AS p1\__x, ay AS p1\__y,
--                  bx AS p2\__x, by AS p2\__y
--           FROM vectors|]
--    :: 'Sq.Statement' 'Sq.Read' () (Point, Point)
-- @
output :: Name -> Output o -> Output o
output n = \case
   Output_Decode bn d ->
      Output_Decode (bindingName n <> bn) (output n <$> d)
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

-- | 'Data.Kind.Constraint' to be satisfied for using 'honput'.
type HOutput h xs = HAsum h xs

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

-- | Default way to decode the 'Output' from a 'Sq.Statement' as a Haskell
-- value of type @a@.
--
-- If there there exist also a 'Sq.InputDefault' instance for @o@, then it
-- must roundtrip with the 'Sq.OutputDefault' instance for @o@.
class OutputDefault o where
   outputDefault :: Output o
