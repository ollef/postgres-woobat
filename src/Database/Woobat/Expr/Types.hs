{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module Database.Woobat.Expr.Types where

import Data.Functor.Identity
import qualified Data.Generic.HKD as HKD
import Data.Kind (Type)
import qualified Database.Woobat.Raw as Raw

newtype Expr a = Expr Raw.Expr

newtype AggregateExpr a = AggregateExpr Raw.Expr

type NullableExpr a = NullableF Expr a

newtype NullableF f a = NullableF (f (Nullable a))

type family Nullable a where
  Nullable (Maybe a) = Maybe a
  Nullable a = Maybe a

type Row row = RowF row Identity
newtype RowF row (f :: Type -> Type) = Row (row f)

instance HKD.FunctorB row => HKD.FunctorB (RowF row) where
  bmap f (Row row_) = Row $ HKD.bmap f row_

instance HKD.TraversableB row => HKD.TraversableB (RowF row) where
  btraverse f (Row row_) = Row <$> HKD.btraverse f row_

instance HKD.ConstraintsB row => HKD.ConstraintsB (RowF row) where
  type AllB c (RowF row) = HKD.AllB c row
  baddDicts (Row row) = Row $ HKD.baddDicts row

deriving instance Eq (row Identity) => Eq (Row row)
deriving instance Ord (row Identity) => Ord (Row row)
deriving instance Show (row Identity) => Show (Row row)
