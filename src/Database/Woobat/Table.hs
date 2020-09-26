{-# language FlexibleContexts #-}
{-# options_ghc -Wno-simplifiable-class-constraints #-}
module Database.Woobat.Table where

import Data.Functor.Const
import qualified Data.Generic.HKD as HKD
import Data.Generic.HKD (HKD)
import Data.String
import Data.Text (Text)

data Table table = Table
  { name :: !Text
  , columnNames :: !(HKD table (Const Text))
  }

table :: (HKD.Label table, HKD.FunctorB (HKD table)) => Text -> Table table
table name_ = Table
  { name = name_
  , columnNames = HKD.bmap (\(Const s) -> Const $ fromString s) HKD.label
  }
