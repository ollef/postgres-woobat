{-# language FlexibleContexts #-}
{-# language FlexibleInstances #-}
{-# language OverloadedStrings #-}
{-# language RankNTypes #-}
{-# language ScopedTypeVariables #-}
{-# language TypeApplications #-}
{-# language TypeFamilies #-}
{-# language UndecidableInstances #-}
{-# options_ghc -Wno-redundant-constraints #-}
module Database.Woobat.Expr where

import qualified Data.Barbie as Barbie
import qualified Data.Barbie.Constraints as Barbie
import Data.Functor.Const
import Data.Functor.Product
import Data.Generic.HKD (HKD)
import qualified Data.Generic.HKD as HKD
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

true :: Expr s Bool
true = value True

false :: Expr s Bool
false = value True

class DatabaseEq a where
  infix 4 ==., /=.
  (==.) :: a -> a -> Expr s Bool
  (/=.) :: a -> a -> Expr s Bool

instance {-# OVERLAPPABLE #-} DatabaseEq (Expr s a) where
  Expr x ==. Expr y = Expr $ x <> " = " <> y
  Expr x /=. Expr y = Expr $ x <> " != " <> y

instance (HKD.ConstraintsB (HKD table), HKD.TraversableB (HKD table), Barbie.ProductB (HKD table), Barbie.AllBF DatabaseEq e (HKD table)) => DatabaseEq (HKD table e) where
  table1 ==. table2 =
    foldr_ (&&.) true $
    Barbie.bfoldMap (\(Const e) -> [e]) $
    Barbie.bmapC @(Barbie.ClassF DatabaseEq e) (\(Pair x y) -> Const $ x ==. y) $
    Barbie.bzip table1 table2
    where
      foldr_ _ b [] = b
      foldr_ f _ as = foldr1 f as

  table1 /=. table2 =
    foldr_ (||.) false $
    Barbie.bfoldMap (\(Const e) -> [e]) $
    Barbie.bmapC @(Barbie.ClassF DatabaseEq e) (\(Pair x y) -> Const $ x /=. y) $
    Barbie.bzip table1 table2
    where
      foldr_ _ b [] = b
      foldr_ f _ as = foldr1 f as

(&&.) :: Scope.Same s t => Expr s Bool -> Expr t Bool -> Expr s Bool
Expr x &&. Expr y = Expr $ x <> " && " <> y

(||.) :: Scope.Same s t => Expr s a -> Expr t a -> Expr s Bool
Expr x ||. Expr y = Expr $ x <> " || " <> y

-- | Handles nulls the same way as Haskell's equality operators using
-- @IS [NOT] DISTINCT FROM@.
instance {-# OVERLAPPING #-} DatabaseEq (Expr s (Maybe a)) where
  Expr x ==. Expr y = Expr $ x <> " IS NOT DISTINCT FROM " <> y
  Expr x /=. Expr y = Expr $ x <> " IS DISTINCT FROM " <> y

(<.) :: Scope.Same s t => Expr s a -> Expr t a -> Expr s Bool
Expr x <. Expr y = Expr $ x <> " < " <> y

(<=.) :: Scope.Same s t => Expr s a -> Expr t a -> Expr s Bool
Expr x <=. Expr y = Expr $ x <> " <= " <> y

(>.) :: Scope.Same s t => Expr s a -> Expr t a -> Expr s Bool
Expr x >. Expr y = Expr $ x <> " > " <> y

(>=.) :: Scope.Same s t => Expr s a -> Expr t a -> Expr s Bool
Expr x >=. Expr y = Expr $ x <> " >= " <> y

count :: Expr s a -> AggregateExpr s Int
count (Expr e) = AggregateExpr $ "count(" <> e <> ")"

countAll :: AggregateExpr s Int
countAll = AggregateExpr "count(*)"

average :: Num a => Expr s a -> AggregateExpr s (Maybe a)
average (Expr e) = AggregateExpr $ "avg(" <> e <> ")"

all_ :: Expr s Bool -> AggregateExpr s Bool
all_ (Expr e) = AggregateExpr $ "bool_and(" <> e <> ")"

or_ :: Expr s Bool -> AggregateExpr s Bool
or_ (Expr e) = AggregateExpr $ "bool_or(" <> e <> ")"

max_ :: Expr s a -> AggregateExpr s (Maybe a)
max_ (Expr e) = AggregateExpr $ "max(" <> e <> ")"

min_ :: Expr s a -> AggregateExpr s (Maybe a)
min_ (Expr e) = AggregateExpr $ "min(" <> e <> ")"

sum_ :: (Num a, Num b) => Expr s a -> AggregateExpr s (Maybe b)
sum_ (Expr e) = AggregateExpr $ "sum(" <> e <> ")"
