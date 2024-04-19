{-# LANGUAGE TemplateHaskell #-}

module Sq.Mode
   ( -- * Mode
    Mode (..)
   , type Read
   , type Write
   , ReadSym0
   , WriteSym0
   , SMode (..)

    -- * TransactionMode
   , TransactionMode (..)
   , type Reading
   , type Committing
   , type Rollbacking
   , ReadingSym0
   , CommittingSym0
   , RollbackingSym0
   , STransactionMode (..)

    -- * Constraints
   , ConnectionSupportsTransaction
   , TransactionCan
   , SubMode
   ) where

import Data.Singletons.TH
import GHC.TypeLits qualified as GHC
import Prelude hiding (Read)

--------------------------------------------------------------------------------

singletons [d|data Mode = Read | Write|]

deriving stock instance Eq Mode
deriving stock instance Ord Mode
deriving stock instance Show Mode

type Read = 'Read
type Write = 'Write

--------------------------------------------------------------------------------

singletons
   [d|
      data TransactionMode
         = -- | Transaction supports read operations only.
           Reading
         | -- | Transaction supports read and write operations. Changes are
           -- commited to the database unless there is an unhandled exception
           -- during the transaction, in which case they are rolled-back.
           Committing
         | -- | Transaction supports read and write operations. However,
           -- changes are always rolled back at the end of the transaction.
           -- This is mostly useful for testing purposes.
           Rollbacking
      |]

deriving stock instance Eq TransactionMode
deriving stock instance Ord TransactionMode
deriving stock instance Show TransactionMode

type Reading = 'Reading
type Committing = 'Committing
type Rollbacking = 'Rollbacking

--------------------------------------------------------------------------------

class TransactionCan (mode :: Mode) (tmode :: TransactionMode)
instance TransactionCan Write Committing
instance TransactionCan Write Rollbacking
instance TransactionCan Read Committing
instance TransactionCan Read Rollbacking
instance TransactionCan Read Reading
instance
   (GHC.TypeError (GHC.Text "Can't write in Reading transaction mode"))
   => TransactionCan Write Reading

class ConnectionSupportsTransaction (cmode :: Mode) (tmode :: TransactionMode)
instance ConnectionSupportsTransaction Write Reading
instance ConnectionSupportsTransaction Write Committing
instance ConnectionSupportsTransaction Write Rollbacking
instance ConnectionSupportsTransaction Read Reading
instance
   (GHC.TypeError (GHC.Text "Read-only connection doesn't support Committing transaction"))
   => ConnectionSupportsTransaction Read Committing
instance
   (GHC.TypeError (GHC.Text "Read-only connection doesn't support Rollbacking transaction"))
   => ConnectionSupportsTransaction Read Rollbacking

class SubMode (sup :: Mode) (sub :: Mode)
instance SubMode Read Read
instance SubMode Write Read
instance SubMode Write Write
instance
   (GHC.TypeError (GHC.Text "Write mode is not a subset of Read mode"))
   => SubMode 'Read 'Write
