{-# LANGUAGE StrictData #-}

module Sq.Names
   ( Name
   , name
   , BindingName
   , bindingName
   , renderInputBindingName
   , renderOutputBindingName
   ) where

import Control.Applicative
import Control.DeepSeq
import Data.Attoparsec.Text qualified as AT
import Data.Char qualified as Ch
import Data.Coerce
import Data.String
import Data.Text qualified as T
import GHC.Records

import Sq.Support

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
name = AT.parseOnly do
   c1 <- AT.satisfy pred1
   yt <- optional ptail
   AT.endOfInput
   pure $ Name $ case yt of
      Just (c2s, c3) -> T.pack (c1 : c2s) <> T.singleton c3
      Nothing -> T.singleton c1
  where
   pred1 = \c -> Ch.isAsciiLower c || Ch.isAsciiUpper c
   pred2 = \c -> pred3 c || c == '_'
   pred3 = \c -> pred1 c || Ch.isDigit c
   ptail = manyTill1 (AT.satisfy pred3 <* AT.endOfInput) (AT.satisfy pred2)

--------------------------------------------------------------------------------

data BindingName = BindingName Name [Name]
   deriving stock (Eq, Ord)

instance Show BindingName where
   showsPrec n = showsPrec n . renderInputBindingName

bindingName :: Name -> BindingName
bindingName n = BindingName n []

instance NFData BindingName where
   rnf (BindingName n ns) = rnf n `seq` rnf ns

instance Semigroup BindingName where
   BindingName a as <> BindingName b bs = BindingName a (as <> (b : bs))

-- | @$foo__bar3__the_thing@
renderInputBindingName :: BindingName -> T.Text
renderInputBindingName (BindingName n ns) =
   T.cons '$' $ T.intercalate "__" $ fmap (.text) (n : ns)

-- | @foo__bar3__the_thing@
renderOutputBindingName :: BindingName -> T.Text
renderOutputBindingName (BindingName n ns) =
   T.intercalate "__" $ fmap (.text) (n : ns)
