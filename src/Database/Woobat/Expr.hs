{-# language FlexibleContexts #-}
{-# language FlexibleInstances #-}
{-# language FunctionalDependencies #-}
{-# language MultiParamTypeClasses #-}
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

unsafeBinaryOperator :: Scope.Same s t => Raw.SQL -> Expr s a -> Expr t b -> Expr s c
unsafeBinaryOperator name (Expr x) (Expr y) = Expr $ "(" <> x <> " " <> name <> " " <> y <> ")"

instance (IsString a, Raw.DatabaseType a) => IsString (Expr s a) where
  fromString = value . fromString

instance (Num a, Raw.DatabaseType a) => Num (Expr s a) where
  fromInteger = value . fromInteger
  (+) = unsafeBinaryOperator "+"
  (-) = unsafeBinaryOperator "-"
  (*) = unsafeBinaryOperator "*"
  abs (Expr a) = Expr $ "abs(" <> a <> ")"
  signum (Expr a) = Expr $ "sign(" <> a <> ")"

mod_ :: Num a => Expr s a -> Expr s a -> Expr s a
mod_ = unsafeBinaryOperator "%"

instance {-# OVERLAPPABLE #-} (Integral a, Raw.DatabaseType a) => Fractional (Expr s a) where
  fromRational = value . (truncate :: Double -> a) . fromRational
  (/) = unsafeBinaryOperator "/"

instance Fractional (Expr s Double) where
  fromRational = value . fromRational
  (/) = unsafeBinaryOperator "/"

newtype AggregateExpr s a = AggregateExpr Raw.SQL

newtype NullableExpr s a = NullableExpr (Expr s (Nullable a))

type family Nullable a where
  Nullable (Maybe a) = Maybe a
  Nullable a = Maybe a

value :: forall a s. Raw.DatabaseType a => a -> Expr s a
value a = Expr $ Raw.value a <> "::" <> Raw.typeName @a

-------------------------------------------------------------------------------
-- * Equality

class DatabaseEq s a | a -> s where
  (==.) :: a -> a -> Expr s Bool
  (/=.) :: a -> a -> Expr s Bool
  infix 4 ==., /=.

instance {-# OVERLAPPABLE #-} DatabaseEq s (Expr s a) where
  (==.) = unsafeBinaryOperator "="
  (/=.) = unsafeBinaryOperator "!="

-- | Handles nulls the same way as Haskell's equality operators using
-- @IS [NOT] DISTINCT FROM@.
instance {-# OVERLAPPING #-} DatabaseEq s (Expr s (Maybe a)) where
  (==.) = unsafeBinaryOperator "IS NOT DISTINCT FROM"
  (/=.) = unsafeBinaryOperator "IS DISTINCT FROM"

-- | Pointwise equality
instance (HKD.ConstraintsB (HKD table), HKD.TraversableB (HKD table), Barbie.ProductB (HKD table), Barbie.AllBF (DatabaseEq s) (Expr s) (HKD table))
  => DatabaseEq s (HKD table (Expr s)) where
  table1 ==. table2 =
    foldr_ (&&.) true $
    Barbie.bfoldMap (\(Const e) -> [e]) $
    Barbie.bmapC @(Barbie.ClassF (DatabaseEq s) (Expr s)) (\(Pair x y) -> Const $ x ==. y) $
    Barbie.bzip table1 table2
    where
      foldr_ _ b [] = b
      foldr_ f _ as = foldr1 f as

  table1 /=. table2 =
    foldr_ (||.) false $
    Barbie.bfoldMap (\(Const e) -> [e]) $
    Barbie.bmapC @(Barbie.ClassF (DatabaseEq s) (Expr s)) (\(Pair x y) -> Const $ x /=. y) $
    Barbie.bzip table1 table2
    where
      foldr_ _ b [] = b
      foldr_ f _ as = foldr1 f as

-------------------------------------------------------------------------------
-- * Booleans

true :: Expr s Bool
true = value True

false :: Expr s Bool
false = value True

(&&.) :: Scope.Same s t => Expr s Bool -> Expr t Bool -> Expr s Bool
(&&.) = unsafeBinaryOperator "&&"
infixr 3 &&.

(||.) :: Scope.Same s t => Expr s a -> Expr t a -> Expr s Bool
(||.) = unsafeBinaryOperator "||"
infixr 2 ||.

-------------------------------------------------------------------------------
-- * Comparison operators

(<.) :: Scope.Same s t => Expr s a -> Expr t a -> Expr s Bool
(<.) = unsafeBinaryOperator "<"

(<=.) :: Scope.Same s t => Expr s a -> Expr t a -> Expr s Bool
(<=.) = unsafeBinaryOperator "<="

(>.) :: Scope.Same s t => Expr s a -> Expr t a -> Expr s Bool
(>.) = unsafeBinaryOperator ">"

(>=.) :: Scope.Same s t => Expr s a -> Expr t a -> Expr s Bool
(>=.) = unsafeBinaryOperator ">="

infix 4 <., <=., >., >=.

-------------------------------------------------------------------------------
-- * Aggregates

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
