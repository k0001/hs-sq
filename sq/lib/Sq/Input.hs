{-# LANGUAGE StrictData #-}

module Sq.Input
   ( Input
   , runInput
   , encode
   , input
   , BoundInput
   , bindInput
   , ErrInput (..)
   , runBoundInput
   ) where

import Control.DeepSeq
import Control.Exception.Safe qualified as Ex
import Control.Monad
import Data.Coerce
import Data.Functor.Contravariant
import Data.Functor.Contravariant.Divisible
import Data.Map.Strict qualified as Map
import Data.String
import Data.Text qualified as T
import Database.SQLite3 qualified as S

import Sq.Encoders
import Sq.Names

--------------------------------------------------------------------------------

-- | Encodes all the input to a single 'Statement'.
--
-- * Construct with 'encode' or 'IsString'.
--
-- * Compose with 'input', 'Contravariant', 'Divisible', 'Decidable',
-- 'Semigroup' or 'Monoid'
newtype Input i = Input (i -> Map.Map BindingName (Either ErrEncode S.SQLData))
   deriving newtype
      ( Semigroup
        -- ^ Left-biased in case of overlapping 'BindingName's.
      , Monoid
        -- ^ Left-biased in case of overlapping 'BindingName's.
      , NFData
      )
   deriving
      ( Contravariant
      , Divisible
        -- ^ Left-biased in case of overlapping 'BindingName's.
      , Decidable
      )
      via Op (Map.Map BindingName (Either ErrEncode S.SQLData))

runInput :: Input i -> i -> Map.Map BindingName (Either ErrEncode S.SQLData)
runInput = coerce
{-# INLINE runInput #-}

-- | Encode a single input parameter. The 'Name' will be reachable from the 'SQL'
-- query with a @$@ prefix.
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
-- Multiple 'Input's can be combined:
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
-- Pro-tip: Consider using the 'IsString' instance for 'Input',
-- where for example @\"foo\"@ means @'encode' \"foo\" 'encodeDefault'@.
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

-- | Add a prefix to all the parameters in the 'Input', separated by @\__@
-- from the rest of the 'Name'.
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
--
-- Multiple 'Input's can be combined using 'Contravariant' tools:
--
-- @
-- 'Sq.writeStatement'
--         ('divided' \"a\" \"b\")
--         'mempty'
--         \"INSERT INTO t (x, y) VALUES ($a, $b)\"
--    :: ('EncodeDefault' a, 'EncodeDefault' b)
--    => 'Sq.Statement' 'Sq.Write' (a, b) ()
-- @
instance (EncodeDefault i) => IsString (Input i) where
   fromString s = encode (fromString s) encodeDefault
   {-# INLINE fromString #-}

--------------------------------------------------------------------------------

newtype BoundInput = BoundInput [(T.Text, S.SQLData)]
   deriving newtype (Eq, Show)

bindInput :: Input i -> i -> Either ErrInput BoundInput
bindInput ii i = fmap BoundInput do
   forM (Map.toAscList (runInput ii i)) \(bn, ev) -> do
      let !k = renderInputBindingName bn
      case ev of
         Right !d -> Right (k, d)
         Left e -> Left $ ErrInput bn e

-- | See v'Encode'.
data ErrInput = ErrInput BindingName ErrEncode
   deriving stock (Show)
   deriving anyclass (Ex.Exception)

runBoundInput :: BoundInput -> [(T.Text, S.SQLData)]
runBoundInput = coerce
{-# INLINE runBoundInput #-}
