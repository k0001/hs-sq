module Sq.Mode
   ( -- * Mode
    Mode (..)
   , SMode (..)
   , fromSMode

    -- * Constraints
   , SubMode
   ) where

import Di.Df1 qualified as Di
import GHC.TypeLits qualified as GHC
import Prelude hiding (Read)

--------------------------------------------------------------------------------

data Mode
   = -- | * A @'Sq.Statement' \''Read'@ performs a read-only query.
     -- Obtain with 'Sq.readStatement'.
     --
     -- * A @'Sq.Transaction' \''Read'@ can permorm \''Read'
     -- 'Sq.Statement's only. Obtain with 'Sq.readTransaction'.
     --
     -- * A @'Sq.Pool' \''Read'@ can permorm \''Read' 'Sq.Transaction's only.
     -- Obtain with 'Sq.readPool' or 'Sq.subPool'.
     Read
   | -- | * A @'Sq.Statement' \''Write'@ performs a read or write query.
     -- Obtain with 'Sq.writeStatement'.
     --
     -- * A @'Sq.Transaction' \''Write'@ can permorm both \''Read' and
     -- \''Write' 'Sq.Statement's. Obtain with 'Sq.commitTransaction' or
     -- 'Sq.rollbackTransaction'.
     --
     -- * A @'Sq.Pool' \''Write'@ can permorm both \''Read' and \''Write'
     -- 'Sq.Transaction's. Obtain with 'Sq.writePool' or 'Sq.tempPool'.
     Write
   deriving stock (Eq, Ord, Show)

data SMode (mode :: Mode) where
   SRead :: SMode 'Read
   SWrite :: SMode 'Write

fromSMode :: SMode mode -> Mode
fromSMode = \case
   SRead -> Read
   SWrite -> Write

instance Di.ToValue Mode where
   value = \case
      Read -> "read"
      Write -> "write"

--------------------------------------------------------------------------------

class SubMode (sup :: Mode) (sub :: Mode)
instance SubMode Read Read
instance SubMode Write Read
instance SubMode Write Write
instance
   (GHC.TypeError (GHC.Text "Write mode is not a subset of Read mode"))
   => SubMode Read Write
