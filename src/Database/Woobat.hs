{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Database.Woobat (
  module Database.Woobat.Expr,
  module Database.Woobat.Monad,
  module Database.Woobat.Scope,
  module Database.Woobat.Select,
  module Database.Woobat.Table,
) where

import Data.Functor.Product
import Database.Woobat.Expr hiding (Impossible, hkdRow, param, unsafeBinaryOperator, unsafeCastFromJSONString)
import Database.Woobat.Monad
import Database.Woobat.Scope
import Database.Woobat.Select
import Database.Woobat.Table (Table, table)

-- TODO define own product type to work around this
instance (Semigroup (f a), Semigroup (g a)) => Semigroup (Product f g a) where
  Pair fa1 ga1 <> Pair fa2 ga2 = Pair (fa1 <> fa2) (ga1 <> ga2)

instance (Monoid (f a), Monoid (g a)) => Monoid (Product f g a) where
  mempty = Pair mempty mempty
