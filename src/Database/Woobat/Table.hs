module Database.Woobat.Table where

import Data.Text (Text)

data Table table = Table
  { name :: !Text
  , fieldNameModifier :: !(Text -> Text)
  }
