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
import GHC.Records
import GHC.Show
import Language.Haskell.TH.Quote (QuasiQuoter (..))
import Prelude hiding (Read, log)

import Sq.Input
import Sq.Mode
import Sq.Names
import Sq.Output

--------------------------------------------------------------------------------

newtype SQL = SQL T.Text
   deriving newtype (Eq, Ord, Show, IsString, Semigroup, NFData)

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

-- | A statements statement taking a value @i@ as input and producing rows of
-- @o@ values as output.
data Statement (mode :: Mode) i o = Statement
   { _input :: Either (Either ErrInput BoundInput) (Input i)
   , _output :: Output o
   , _sql :: SQL
   }

instance HasField "sql" (Statement mode i o) SQL where
   getField = (._sql)

instance Show (Statement mode i o) where
   showsPrec n s =
      showParen (n >= appPrec1) $
         showString "Statement{sql = "
            . shows s._sql
            . showString ", input = "
            . either shows (\_ -> showString "..") s._input
            . showString ", output = ..}"

statement
   :: forall mode i o
    . Input i
   -> Output o
   -> SQL
   -> Statement mode i o
statement ii _output _sql = Statement{_input = Right ii, ..}
{-# INLINE statement #-}

instance Functor (Statement mode i) where
   fmap = rmap
   {-# INLINE fmap #-}

instance Profunctor (Statement mode) where
   dimap f g st =
      st{_input = fmap (contramap f) st._input, _output = fmap g st._output}
   {-# INLINE dimap #-}

-- | This function is run automatically by 'rowStream' and similar @rowXxx@
-- functions before acquiring the 'Transaction'. You never need to call this
-- function yourself. However, if the 'Transaction' is acquired manually to be
-- reused in multiple queries, then you can use 'bindStatement' before
-- acquiring said 'Transaction' in order to make sure that the 'Input' encoding
-- work happens before the 'Transaction' is locked. This only makes sense if
-- the 'Input' encoding work is expected to be expensive.
--
-- Note: Make sure to evaluate the resulting 'Statement' to WHNF.
bindStatement :: Statement mode i o -> i -> Statement mode () o
bindStatement st i = case runStatementInput st i of
   Right !bi -> st{_input = Left (Right bi)}
   Left ei -> st{_input = Left (Left ei)}
{-# INLINE bindStatement #-}

runStatementInput :: Statement mode i o -> i -> Either ErrInput BoundInput
runStatementInput st i = either id (flip bindInput i) st._input
{-# INLINE runStatementInput #-}

runStatementOutput
   :: (Monad m)
   => Statement mode i o
   -> (Name -> m (Maybe S.SQLData))
   -> m (Either ErrOutput o)
runStatementOutput st f = runOutput f st._output
{-# INLINE runStatementOutput #-}
