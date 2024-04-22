{-# LANGUAGE StrictData #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoFieldSelectors #-}

module Sq.Statement
   ( SQL
   , sql
   , Statement (..)
   , statement
   , bindStatement
   , runStatementInput
   , runStatementOutput
   ) where

import Control.DeepSeq
import Control.Monad
import Data.Coerce
import Data.Functor.Contravariant
import Data.Profunctor
import Data.String
import Data.Text qualified as T
import Database.SQLite3 qualified as S
import Di.Df1 qualified as Di
import GHC.Records
import GHC.Show
import Language.Haskell.TH.Quote (QuasiQuoter (..))
import Prelude hiding (Read, log)

import Sq.Input
import Sq.Mode
import Sq.Names
import Sq.Output

--------------------------------------------------------------------------------

-- | Raw SQL String. Completely unsafe.
newtype SQL = SQL T.Text
   deriving newtype (Eq, Ord, Show, IsString, Semigroup, NFData)

instance Di.ToMessage SQL where
   message = Di.message . show

instance HasField "text" SQL T.Text where getField = coerce

sql :: QuasiQuoter
sql =
   QuasiQuoter
      { quoteExp = \s -> [|fromString @SQL s|]
      , quotePat = \_ -> fail "sql: No quotePat"
      , quoteType = \_ -> fail "sql: No quoteType"
      , quoteDec = \_ -> fail "sql: No quoteDec"
      }

--------------------------------------------------------------------------------

-- | * A statement taking a value @i@ as input and producing rows of
-- @o@ values as output.
--
-- * @s@ indicates whether 'Read'-only or read-'Write' 'Statement's are
-- supported.
--
-- * Construct with 'Sq.readStatement' or 'Sq.writeStatement'.
data Statement (s :: Mode) i o = Statement
   { _input :: Either (Either ErrInput BoundInput) (Input i)
   , _output :: Output o
   , _sql :: SQL
   -- TODO: _cache :: Bool
   }

instance HasField "sql" (Statement s i o) SQL where
   getField = (._sql)

instance Show (Statement s i o) where
   showsPrec n s =
      showParen (n >= appPrec1) $
         showString "Statement{sql = "
            . shows s._sql
            . showString ", input = "
            . either shows (\_ -> showString "..") s._input
            . showString ", output = ..}"

statement
   :: forall s i o
    . Input i
   -> Output o
   -> SQL
   -> Statement s i o
statement ii _output _sql = Statement{_input = Right ii, ..}
{-# INLINE statement #-}

instance Functor (Statement s i) where
   fmap = rmap
   {-# INLINE fmap #-}

instance Profunctor (Statement s) where
   dimap f g st =
      st{_input = fmap (contramap f) st._input, _output = fmap g st._output}
   {-# INLINE dimap #-}

-- | You can use this  function if you have the 'Statement' input readily
-- available sooner than necessary, or if you expect that binding the input
-- will be a costly operation so you would like to make sure that such work
-- happens outside of a 'Transaction'.
--
-- In you are intersted in those later strictness benefits, then make sure to
-- evaluate the resulting 'Statement' to WHNF.
bindStatement :: Statement s i o -> i -> Statement s () o
bindStatement st i = case runStatementInput st i of
   Right !bi -> st{_input = Left (Right bi)}
   Left ei -> st{_input = Left (Left ei)}
{-# INLINE bindStatement #-}

runStatementInput :: Statement s i o -> i -> Either ErrInput BoundInput
runStatementInput st i = either id (flip bindInput i) st._input
{-# INLINE runStatementInput #-}

runStatementOutput
   :: (Monad m)
   => Statement s i o
   -> (BindingName -> m (Maybe S.SQLData))
   -> m (Either ErrOutput o)
runStatementOutput st f = runOutput f st._output
{-# INLINE runStatementOutput #-}
