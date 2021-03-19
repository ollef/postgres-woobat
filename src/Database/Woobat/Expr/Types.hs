{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}

module Database.Woobat.Expr.Types where

import qualified Barbies
import Data.Functor.Identity
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

instance Barbies.FunctorB row => Barbies.FunctorB (RowF row) where
  bmap f (Row row_) = Row $ Barbies.bmap f row_

instance Barbies.TraversableB row => Barbies.TraversableB (RowF row) where
  btraverse f (Row row_) = Row <$> Barbies.btraverse f row_

instance Barbies.ConstraintsB row => Barbies.ConstraintsB (RowF row) where
  type AllB c (RowF row) = Barbies.AllB c row
  baddDicts (Row row) = Row $ Barbies.baddDicts row

deriving instance Eq (row Identity) => Eq (Row row)
deriving instance Ord (row Identity) => Ord (Row row)
deriving instance Show (row Identity) => Show (Row row)
