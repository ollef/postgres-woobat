{-# LANGUAGE TypeFamilies #-}

module Database.Woobat.Expr.Types where

import qualified Database.Woobat.Raw as Raw

newtype Expr s a = Expr Raw.Expr

newtype AggregateExpr s a = AggregateExpr Raw.Expr

type NullableExpr s a = NullableF (Expr s) a

newtype NullableF f a = NullableF (f (Nullable a))

type family Nullable a where
  Nullable (Maybe a) = Maybe a
  Nullable a = Maybe a
