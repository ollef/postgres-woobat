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

import qualified ByteString.StrictBuilder as Builder
import qualified Data.Barbie as Barbie
import qualified Data.Barbie.Constraints as Barbie
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as Lazy
import Data.Coerce
import Data.Functor.Const
import Data.Functor.Identity
import Data.Functor.Product
import Data.Generic.HKD (HKD)
import qualified Data.Generic.HKD as HKD
import Data.Int
import Data.List
import Data.Scientific
import Data.String (IsString, fromString)
import Data.Text (Text)
import qualified Data.Text.Lazy as Lazy
import Data.Time (Day, TimeOfDay, LocalTime, UTCTime, DiffTime, TimeZone)
import Data.UUID.Types (UUID)
import Data.Word
import qualified Database.Woobat.Raw as Raw
import qualified Database.Woobat.Scope as Scope
import PostgreSQL.Binary.Encoding (Encoding)
import qualified PostgreSQL.Binary.Encoding as Encoding

-------------------------------------------------------------------------------
-- * Types

newtype Expr s a = Expr Raw.SQL

newtype AggregateExpr s a = AggregateExpr Raw.SQL

newtype NullableExpr s a = NullableExpr (Expr s (Nullable a))

type family Nullable a where
  Nullable (Maybe a) = Maybe a
  Nullable a = Maybe a

-------------------------------------------------------------------------------
-- * Strings

instance (IsString a, DatabaseType a) => IsString (Expr s a) where
  fromString = value . fromString

-------------------------------------------------------------------------------
-- * Numerics

instance (Num a, DatabaseType a) => Num (Expr s a) where
  fromInteger = value . fromInteger
  (+) = unsafeBinaryOperator "+"
  (-) = unsafeBinaryOperator "-"
  (*) = unsafeBinaryOperator "*"
  abs (Expr a) = Expr $ "abs(" <> a <> ")"
  signum (Expr a) = Expr $ "sign(" <> a <> ")"

mod_ :: Num a => Expr s a -> Expr s a -> Expr s a
mod_ = unsafeBinaryOperator "%"

instance {-# OVERLAPPABLE #-} (Integral a, DatabaseType a) => Fractional (Expr s a) where
  fromRational = value . (truncate :: Double -> a) . fromRational
  (/) = unsafeBinaryOperator "/"

instance Fractional (Expr s Double) where
  fromRational = value . fromRational
  (/) = unsafeBinaryOperator "/"


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
instance
  (HKD.ConstraintsB (HKD table), HKD.TraversableB (HKD table), Barbie.ProductB (HKD table), Barbie.AllBF (DatabaseEq s) (Expr s) (HKD table))
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

-------------------------------------------------------------------------------
-- * Rows

row :: HKD.TraversableB (HKD table) => HKD table (Expr s) -> Expr s table
row table = Expr $
  "ROW(" <>
    mconcat (intersperse ", " $ Barbie.bfoldMap (\(Expr e) -> [e]) table) <>
  ")"

-------------------------------------------------------------------------------
-- * Going from Haskell types to database types and back

class DatabaseType a where
  value :: a -> Expr s a

-- | Arrays
instance DatabaseType a => DatabaseType [a] where
  value as =
    Expr $ "ARRAY[" <> mconcat (intersperse ", " $ map ((\(Expr sql) -> sql) . value) as) <> "]"

-- | Rows
instance {-# OVERLAPPABLE #-}
  (HKD.Construct Identity table, HKD.ConstraintsB (HKD table), HKD.TraversableB (HKD table), Barbie.AllB DatabaseType (HKD table))
  => DatabaseType table where
  value table =
    row $ Barbie.bmapC @DatabaseType (\(Identity field) -> value field) $ HKD.deconstruct table

-- | Nullable types
-- TODO disallow nested maybes
instance DatabaseType a => DatabaseType (Maybe a) where
  value Nothing = Expr Raw.nullParam
  value (Just a) = coerce $ value a

-- | @boolean@
instance DatabaseType Bool where
  value = param "boolean" Encoding.bool

-- | @integer@
instance DatabaseType Int where
  value = param "integer" $ Encoding.int4_int32 . fromIntegral

-- | @int2@
instance DatabaseType Int16 where
  value = param "int2" Encoding.int2_int16

-- | @int4@
instance DatabaseType Int32 where
  value = param "int4" Encoding.int4_int32

-- | @int8@
instance DatabaseType Int64 where
  value = param "int8" Encoding.int8_int64

-- | @int2@
instance DatabaseType Word16 where
  value = param "int2" Encoding.int2_word16

-- | @int4@
instance DatabaseType Word32 where
  value = param "int4" Encoding.int4_word32

-- | @int8@
instance DatabaseType Word64 where
  value = param "int8" Encoding.int8_word64

-- | @float4@
instance DatabaseType Float where
  value = param "float4" Encoding.float4

-- | @float8@
instance DatabaseType Double where
  value = param "float8" Encoding.float8

-- | @numeric@
instance DatabaseType Scientific where
  value = param "numeric" Encoding.numeric

-- | @uuid@
instance DatabaseType UUID where
  value = param "uuid" Encoding.uuid

-- | @character@
instance DatabaseType Char where
  value = param "character" Encoding.char_utf8

-- | @text@
instance DatabaseType Text where
  value = param "text" Encoding.text_strict

-- | @text@
instance DatabaseType Lazy.Text where
  value = param "text" Encoding.text_lazy

-- | @bytea@
instance DatabaseType ByteString where
  value = param "bytea" Encoding.bytea_strict

-- | @bytea@
instance DatabaseType Lazy.ByteString where
  value = param "bytea" Encoding.bytea_lazy

-- | @date@
instance DatabaseType Day where
  value = param "date" Encoding.date

-- | @time@
instance DatabaseType TimeOfDay where
  value = param "time" Encoding.time_int

-- | @timetz@
instance DatabaseType (TimeOfDay, TimeZone) where
  value = param "timetz" Encoding.timetz_int

-- | @timestamp@
instance DatabaseType LocalTime where
  value = param "timestamp" Encoding.timestamp_int

-- | @timestamptz@
instance DatabaseType UTCTime where
  value = param "timestamptz" Encoding.timestamptz_int

-- | @interval@
instance DatabaseType DiffTime where
  value = param "interval" Encoding.interval_int

-------------------------------------------------------------------------------
-- * Low-level utilities

param :: Raw.SQL -> (a -> Encoding) -> a -> Expr s a
param typeName encoding a =
  Expr $ Raw.param (Builder.builderBytes $ encoding a) <> "::" <> typeName

unsafeBinaryOperator :: Scope.Same s t => Raw.SQL -> Expr s a -> Expr t b -> Expr s c
unsafeBinaryOperator name (Expr x) (Expr y) = Expr $ "(" <> x <> " " <> name <> " " <> y <> ")"
