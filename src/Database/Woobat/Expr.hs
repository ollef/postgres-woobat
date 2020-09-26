{-# language FlexibleInstances #-}
{-# language OverloadedStrings #-}
{-# language RankNTypes #-}
{-# language ScopedTypeVariables #-}
{-# language TypeApplications #-}
{-# language TypeFamilies #-}
{-# options_ghc -Wno-redundant-constraints #-}
module Database.Woobat.Expr where

import Data.String (IsString, fromString)
import qualified Database.Woobat.Raw as Raw
import qualified Database.Woobat.Scope as Scope

newtype Expr s a = Expr Raw.SQL

instance (IsString a, Raw.DatabaseType a) => IsString (Expr s a) where
  fromString = value . fromString

instance (Num a, Raw.DatabaseType a) => Num (Expr s a) where
  fromInteger = value . fromInteger
  Expr a + Expr b = Expr $ "(" <> a <> " + " <> b <> ")"
  Expr a - Expr b = Expr $ "(" <> a <> " - " <> b <> ")"
  Expr a * Expr b = Expr $ "(" <> a <> " * " <> b <> ")"
  abs (Expr a) = Expr $ "abs(" <> a <> ")"
  signum (Expr a) = Expr $ "sign(" <> a <> ")"

instance {-# OVERLAPPABLE #-} (Integral a, Raw.DatabaseType a) => Fractional (Expr s a) where
  fromRational = value . (truncate :: Double -> a) . fromRational
  Expr a / Expr b = Expr $ "(" <> a <> " / " <> b <> ")"

instance Fractional (Expr s Double) where
  fromRational = value . fromRational
  Expr a / Expr b = Expr $ "(" <> a <> " / " <> b <> ")"

newtype AggregateExpr s a = AggregateExpr Raw.SQL

newtype NullableExpr s a = NullableExpr (Expr s (Nullable a))

type family Nullable a where
  Nullable (Maybe a) = Maybe a
  Nullable a = Maybe a

value :: forall a s. Raw.DatabaseType a => a -> Expr s a
value a = Expr $ Raw.value a <> "::" <> Raw.typeName @a

(==.) :: Scope.Same s t => Expr s a -> Expr t a -> Expr s Bool
Expr f ==. Expr g =
  Expr $ f <> " = " <> g

infix 4 ==.
