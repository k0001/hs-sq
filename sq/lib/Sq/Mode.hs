{-# LANGUAGE TemplateHaskell #-}

module Sq.Mode
   ( -- * Mode
    Mode (..)
   , ReadSym0
   , WriteSym0
   , SMode (..)

    -- * Release
   , Release (..)

    -- * Constraints
   , SubMode
   ) where

import Data.Singletons.TH
import Di.Df1 qualified as Di
import GHC.TypeLits qualified as GHC
import Prelude hiding (Read)

--------------------------------------------------------------------------------

singletons [d|data Mode = Read | Write|]

deriving stock instance Eq Mode
deriving stock instance Ord Mode
deriving stock instance Show Mode

instance Di.ToValue Mode where
   value = \case
      Read -> "read"
      Write -> "write"

--------------------------------------------------------------------------------

-- | How to release a 'Transaction'.
data Release
   = -- | Changes are commited to the database unless there is an unhandled
     -- exception during the transaction, in which case they are rolled-back.
     Commit
   | -- | Changes are always rolled back at the end of the transaction. This is
     -- mostly useful for testing purposes. Notice that an equivalent behavior
     -- can be achieved by 'Control.Exception.Safe.bracket'ing changes between
     -- 'Sq.savepoint' and 'Sq.rollbackTo' in a 'Commit'ting transaction.
     -- However, using 'Rollback' is much faster.
     Rollback
   deriving stock (Eq, Ord, Show)

instance Di.ToValue Release where
   value = \case
      Commit -> "commit"
      Rollback -> "rollback"

--------------------------------------------------------------------------------

class SubMode (sup :: Mode) (sub :: Mode)
instance SubMode Read Read
instance SubMode Write Read
instance SubMode Write Write
instance
   (GHC.TypeError (GHC.Text "Write mode is not a subset of Read mode"))
   => SubMode Read Write
