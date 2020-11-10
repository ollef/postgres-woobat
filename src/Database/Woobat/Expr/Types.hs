{-# LANGUAGE TypeFamilies #-}

module Database.Woobat.Expr.Types where

import qualified Database.Woobat.Raw as Raw

newtype Expr a = Expr Raw.Expr

newtype AggregateExpr a = AggregateExpr Raw.Expr

type NullableExpr a = NullableF Expr a

newtype NullableF f a = NullableF (f (Nullable a))

type family Nullable a where
  Nullable (Maybe a) = Maybe a
  Nullable a = Maybe a
