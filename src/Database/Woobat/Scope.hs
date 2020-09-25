{-# language DataKinds #-}
{-# language FlexibleInstances #-}
{-# language GADTs #-}
{-# language MultiParamTypeClasses #-}
{-# language OverloadedStrings #-}
{-# language UndecidableInstances #-}
module Database.Woobat.Scope where

import GHC.TypeLits (TypeError)
import qualified GHC.TypeLits as TypeLits

class s ~ t => Same s t where

instance {-# OVERLAPPING #-} Same s s
instance {-# OVERLAPPABLE #-}
  ( s ~ t
  , TypeError ('TypeLits.Text "An identifier from an outer scope may not be used in an inner query.")
  ) => Same s t

data Inner s
