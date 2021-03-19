{-# LANGUAGE FlexibleContexts #-}

module Database.Woobat.Table where

import qualified Barbies
import Data.Functor.Const
import Data.Generic.HKD (HKD)
import qualified Data.Generic.HKD as HKD
import Data.String
import Data.Text (Text)

data Table table = Table
  { name :: !Text
  , columnNames :: !(table (Const Text))
  }

hkdTable :: (HKD.Label table, Barbies.FunctorB (HKD table)) => Text -> Table (HKD table)
hkdTable name_ =
  Table
    { name = name_
    , columnNames = HKD.bmap (\(Const s) -> Const $ fromString s) HKD.label
    }
