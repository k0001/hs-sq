{-# LANGUAGE StrictData #-}

module Sq.Input
   ( Input
   , runInput
   , encode
   , input
   , hinput
   , HInput
   , BoundInput
   , bindInput
   , ErrInput (..)
   , rawBoundInput
   ) where

import Control.DeepSeq
import Control.Exception.Safe qualified as Ex
import Data.Bifunctor
import Data.Coerce
import Data.Functor.Contravariant
import Data.Functor.Contravariant.Divisible
import Data.Functor.Contravariant.Rep
import Data.Map.Strict qualified as Map
import Data.Profunctor
import Data.SOP qualified as SOP
import Data.SOP.Constraint qualified as SOP
import Data.String
import Data.Text qualified as T
import Database.SQLite3 qualified as S

import Sq.Encoders
import Sq.Names

--------------------------------------------------------------------------------

-- | How to encode all the input to a single 'Sq.Statement'.
--
-- * Construct with 'encode', 'IsString'.
--
-- * Nest with 'input'.
--
-- * Compose with 'Contravariant', 'Divisible', 'Decidable' and 'Monoid' tools.
newtype Input i = Input (i -> Map.Map BindingName (Either ErrEncode S.SQLData))
   deriving newtype
      ( Semigroup
        -- ^ Left-biased in case of overlapping 'BindingName's.
      , Monoid
      , NFData
      )
   deriving
      ( Contravariant
      , Divisible
        -- ^ Left-biased in case of overlapping 'BindingName's.
      , Decidable
      )
      via Op (Map.Map BindingName (Either ErrEncode S.SQLData))

instance Representable Input where
   type Rep Input = Map.Map BindingName (Either ErrEncode S.SQLData)
   tabulate = Input
   index = runInput

runInput :: Input i -> i -> Map.Map BindingName (Either ErrEncode S.SQLData)
runInput = coerce
{-# INLINE runInput #-}

-- | Encode a single input parameter. The value will be reachable from the 'SQL'
-- query through the specified 'Name', with a @$@ prefix.
--
-- @
-- 'Sq.writeStatement'
--         ('encode' \"foo\" 'encodeDefault')
--         'mempty'
--         \"INSERT INTO t (a) VALUES ($foo)\"
--    :: ('EncodeDefault' x)
--    => 'Sq.Statement' 'Sq.Write' x ()
-- @
--
-- Note that by design, this library doesn't support positional 'Input'
-- parameters. You must always pick a 'Name'.
--
-- Multiple 'Input's can be composed with 'Contravariant', 'Divisible', 'Decidable'
-- and 'Monoid' tools.
--
-- @
-- 'Sq.writeStatement'
--         ('divided' ('encode' \"foo\" 'encodeDefault')
--                  ('encode' \"bar\" 'encodeDefault'))
--         'mempty'
--         \"INSERT INTO t (a, b) VALUES ($foo, $bar)\"
--    :: ('EncodeDefault' x, 'EncodeDefault' y)
--    => 'Sq.Statement' 'Sq.Write' (x, y) ()
-- @
--
-- Pro-tip: Consider using the 'IsString' instance for 'Input'.
-- For example, @\"foo\"@ means @'encode' \"foo\" 'encodeDefault'@.
-- That is, the last example could be written as follows:
--
-- @
-- 'Sq.writeStatement'
--         ('divided' \"foo\" \"bar\")
--         'mempty'
--         \"INSERT INTO t (a, b) VALUES ($foo, $bar)\"
--    :: ('EncodeDefault' x, 'EncodeDefault' y)
--    => 'Sq.Statement' 'Sq.Write' (x, y) ()
-- @
encode :: Name -> Encode i -> Input i
encode n (Encode f) = Input (Map.singleton (bindingName n) . f)
{-# INLINE encode #-}

-- | Add a prefix 'Name' to parameters names in the given 'Input',
-- separated by @\__@
--
-- This is useful for making reusable 'Input's. For example,
-- consider the following.
--
-- @
-- data Point = Point { x :: 'Int', y :: 'Int' }
--
-- pointInput :: 'Input' Point
-- pointInput = 'contramap' (\\case Point x _ -> x) \"x\" <>
--              'contramap' (\\case Point _ y -> y) \"y\"
-- @
--
-- After 'input':
--
-- @
-- 'Sq.writeStatement'
--         ('divided' ('input' \"p1\" pointInput)
--                  ('input' \"p2\" pointInput))
--         'mempty'
--         ['Sq.sql'|
--           INSERT INTO vectors (ax, ay, bx, by)
--           VALUES ($p1\__x, $p1\__y, $p2\__x, $p2\__y) |]
--    :: 'Sq.Statement' 'Sq.Write' (Point, Point) ()
-- @
input :: Name -> Input i -> Input i
input n ba = Input \s ->
   Map.mapKeysMonotonic (bindingName n <>) (runInput ba s)
{-# INLINE input #-}

-- |
-- @
-- 'Sq.writeStatement'
--         \"a\"
--         'mempty'
--         \"INSERT INTO t (x) VALUES ($a)\"
--    :: ('EncodeDefault' a)
--    => 'Sq.Statement' 'Sq.Write' a ()
-- @
instance (EncodeDefault i) => IsString (Input i) where
   fromString s = encode (fromString s) encodeDefault
   {-# INLINE fromString #-}

--------------------------------------------------------------------------------

newtype BoundInput = BoundInput (Map.Map T.Text S.SQLData)
   deriving newtype (Eq, Show)

bindInput :: Input i -> i -> Either ErrInput BoundInput
bindInput ii i = do
   !m <-
      Map.mapKeysMonotonic renderInputBindingName
         <$> Map.traverseWithKey (first . ErrInput) (runInput ii i)
   pure $ BoundInput m

-- | See v'Encode'.
data ErrInput = ErrInput BindingName ErrEncode
   deriving stock (Show)
   deriving anyclass (Ex.Exception)

rawBoundInput :: BoundInput -> Map.Map T.Text S.SQLData
rawBoundInput = coerce
{-# INLINE rawBoundInput #-}

--------------------------------------------------------------------------------

-- | 'Data.Kind.Constraint' to be satisfied for using 'hinput'.
type HInput h xs =
   ( SOP.AllN h SOP.Top xs
   , SOP.HAp (SOP.Prod h)
   , SOP.HAp h
   , SOP.HTraverse_ h
   , SOP.SListIN (SOP.Prod h) xs
   )

-- | Given a "Data.SOP" 'SOP.Prod'uct containing the 'Input's for encoding each
-- of @xs@, obtain an 'Input' able to encode any of 'SOP.NS', 'SOP.NP',
-- 'SOP.SOP' or 'SOP.POP' for that same @xs@.
--
-- You can see 'hinput' as an alternative 'divide', 'choose' or a combination
-- of those for types other than '(,)' and 'Either'.
hinput :: (HInput h xs) => SOP.Prod h Input xs -> Input (h SOP.I xs)
hinput (ph :: SOP.Prod h Input xs) =
   Input (SOP.hcfoldMap (SOP.Proxy @SOP.Top) SOP.unK . g)
  where
   g :: h SOP.I xs -> h (SOP.K (Rep Input)) xs
   g = SOP.hap (SOP.hmap f ph)
   f :: Input a -> (SOP.I SOP.-.-> SOP.K (Rep Input)) a
   f = SOP.fn . dimap SOP.unI SOP.K . runInput
