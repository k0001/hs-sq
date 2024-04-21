{-# LANGUAGE StrictData #-}

module Sq.Names
   ( Name
   , name
   , BindingName
   , bindingName
   , renderInputBindingName
   , parseInputBindingName
   , renderOutputBindingName
   , parseOutputBindingName
   ) where

import Control.Applicative
import Control.DeepSeq
import Control.Monad
import Data.List.NonEmpty (NonEmpty(..))
import Data.Attoparsec.Text qualified as AT
import Data.Char qualified as Ch
import Data.Coerce
import Data.String
import Data.Text qualified as T
import GHC.Records

--------------------------------------------------------------------------------

newtype Name = Name T.Text
   deriving newtype (Eq, Ord, Show, NFData)

instance IsString Name where
   fromString = either error id . name . T.pack

instance HasField "text" Name T.Text where getField = coerce

-- | * First character must be ASCII letter.
--
-- * Last character, if any, must be ASCII letter or ASCII digit.
--
-- * Characters between the first and last, if any, must be ASCII letters,
-- ASCII digits, or underscore.
name :: T.Text -> Either String Name
name = AT.parseOnly (pName <* AT.endOfInput)

pName :: AT.Parser Name
pName = flip (AT.<?>) "pName" do
   c1 <- AT.satisfy pw
   cs <- ptail
   pure $ Name $ T.pack (c1 : cs)
  where
   pw = \c -> Ch.isAsciiLower c || Ch.isAsciiUpper c
   ptail = many do
      AT.satisfy pw
         <|> AT.satisfy Ch.isDigit
         <|> (AT.char '_' <* (AT.peekChar' >>= \c -> guard (c /= '_')))

--------------------------------------------------------------------------------

-- | A non-empty list of 'Name's that can be rendered as 'Sq.Input' or
-- 'Sq.Output' parameters in a 'Sq.Statement'.
newtype BindingName = BindingName (NonEmpty Name)
   deriving newtype (Eq, Ord, Show, NFData, Semigroup)

bindingName :: Name -> BindingName
bindingName = BindingName . pure

--------------------------------------------------------------------------------

-- | @foo__bar3__the_thing@
renderInputBindingName :: BindingName -> T.Text
renderInputBindingName = T.cons '$' . renderOutputBindingName

parseInputBindingName :: T.Text -> Either String BindingName
parseInputBindingName = AT.parseOnly (pInputBindingName <* AT.endOfInput)

pInputBindingName :: AT.Parser BindingName
pInputBindingName = flip (AT.<?>) "pInputBindingName" do
   void $ AT.char '$'
   AT.sepBy' pName "__" >>= \case
      n : ns -> pure $ BindingName (n :| ns)
      [] -> empty

-- | @foo__bar3__the_thing@
renderOutputBindingName :: BindingName -> T.Text
renderOutputBindingName (BindingName (n :| ns)) =
   T.intercalate "__" $ fmap (.text) (n : ns)

-- | @foo__bar3__the_thing@
parseOutputBindingName :: T.Text -> Either String BindingName
parseOutputBindingName = AT.parseOnly (pOutputBindingName <* AT.endOfInput)

pOutputBindingName :: AT.Parser BindingName
pOutputBindingName = flip (AT.<?>) "pOutputBindingName" do
   AT.sepBy' pName "__" >>= \case
      n : ns -> pure $ BindingName (n :| ns)
      [] -> empty
