{-# LANGUAGE StrictData #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE NoFieldSelectors #-}

module Sq.Statement
   ( SQL
   , sql
   , Statement (..)
   , readStatement
   , writeStatement
   , BoundStatement (..)
   , bindStatement
   ) where

import Control.DeepSeq
import Control.Monad
import Data.Coerce
import Data.Functor.Contravariant
import Data.Profunctor
import Data.String
import Data.Text qualified as T
import Di.Df1 qualified as Di
import GHC.Records
import GHC.Show
import Language.Haskell.TH.Quote (QuasiQuoter (..))
import Prelude hiding (Read, log)

import Sq.Input
import Sq.Mode
import Sq.Output

--------------------------------------------------------------------------------

-- | Raw SQL string. Completely unchecked.
newtype SQL = SQL T.Text
   deriving newtype
      ( Eq
      , Ord
      , -- | Raw SQL string.
        Show
      , IsString
      , Semigroup
      , NFData
      )

instance Di.ToMessage SQL where
   message = Di.message . show

-- | Raw SQL string as 'T.Text'.
instance HasField "text" SQL T.Text where getField = coerce

-- | A 'QuasiQuoter' for raw SQL strings.
--
-- __WARNING:__ This doesn't check the validity of the SQL. It is offered simply
-- because writing multi-line strings in Haskell is otherwise very annoying.
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
-- * @s@ indicates whether the statement is 'Read'-only or read-'Write'.
--
-- * Construct with 'readStatement' or 'writeStatement'.

-- Note: We don't export this constructor because 'readStatement' and
-- 'writeStatement' because it lead to better type inferrence, force users to
-- make a conscious choice about whether they are dealing with a 'Read' or
-- 'Write' statement, and prevent type system from accidentally inferring a
-- 'Write' mode for a 'Read' only statement, which would restrict its
-- usability.
data Statement (s :: Mode) i o = Statement
   { input :: Input i
   , output :: Output o
   , sql :: SQL
   }

instance Show (Statement s i o) where
   showsPrec n s =
      showParen (n >= appPrec1) $
         showString "Statement{sql = "
            . shows s.sql
            . showString ", input = .., output = ..}"

-- | Construct a 'Read'-only 'Statement'.
--
-- __WARNING__: This library doesn't __yet__ provide a safe way to construct
-- 'Statement's. You can potentially write anything in your 'SQL' string.
-- Don't do that.
--
-- * The 'SQL' must be read-only.
--
-- * The 'SQL' must contain a single statement.
--
-- * The 'SQL' must not contain any transaction nor savepoint management
-- statements.
readStatement :: Input i -> Output o -> SQL -> Statement 'Read i o
readStatement = Statement
{-# INLINE readStatement #-}

-- | Construct a 'Statement' that can only be executed as part of a 'Write'
-- 'Transaction'.
--
-- __WARNING__: This library doesn't __yet__ provide a safe way to construct
-- 'Statement's. You can potentially write anything in your 'SQL' string.
-- Don't do that.
--
-- * The 'SQL' must contain a single statement.
--
-- * The 'SQL' must not contain any transaction nor savepoint management
-- statements.
writeStatement :: Input i -> Output o -> SQL -> Statement 'Write i o
writeStatement = Statement
{-# INLINE writeStatement #-}

instance Functor (Statement s i) where
   fmap = rmap
   {-# INLINE fmap #-}

instance Profunctor (Statement s) where
   dimap f g (Statement i o s) = Statement (f >$< i) (g <$> o) s
   {-# INLINE dimap #-}

--------------------------------------------------------------------------------

data BoundStatement (s :: Mode) o = BoundStatement
   { input :: BoundInput
   , output :: Output o
   , sql :: SQL
   }

instance Show (BoundStatement s o) where
   showsPrec n s =
      showParen (n >= appPrec1) $
         showString "Statement{sql = "
            . shows s.sql
            . showString ", input = "
            . shows s.input
            . showString ", output = ..}"

bindStatement :: Statement s i o -> i -> Either ErrInput (BoundStatement s o)
bindStatement st i = do
   bi <- bindInput st.input i
   pure BoundStatement{input = bi, output = st.output, sql = st.sql}
{-# INLINE bindStatement #-}
