{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module Database.Woobat.Expr where

import qualified ByteString.StrictBuilder as Builder
import Control.Monad
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
import Data.Kind (Constraint)
import Data.List
import Data.Scientific
import Data.String (IsString, fromString)
import Data.Text (Text)
import qualified Data.Text.Lazy as Lazy
import Data.Time (Day, DiffTime, LocalTime, TimeOfDay, TimeZone, UTCTime)
import Data.UUID.Types (UUID)
import Data.Word
import qualified Database.Woobat.Raw as Raw
import qualified Database.Woobat.Scope as Scope
import GHC.Generics
import GHC.TypeLits (ErrorMessage (..), TypeError)
import qualified PostgreSQL.Binary.Decoding as Decoding
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

instance Semigroup (Expr s Text) where
  (<>) = unsafeBinaryOperator "||"

instance (DatabaseType a, Semigroup (Expr s a), Monoid a) => Monoid (Expr s a) where
  mempty = value mempty

-------------------------------------------------------------------------------

-- * Numerics

instance (Num a, DatabaseType a) => Num (Expr s a) where
  fromInteger = value . fromInteger
  (+) = unsafeBinaryOperator "+"
  (-) = unsafeBinaryOperator "-"
  (*) = unsafeBinaryOperator "*"
  abs (Expr a) = Expr $ "ABS(" <> a <> ")"
  signum (Expr a) = Expr $ "SIGN(" <> a <> ")"

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

{- | Handles nulls the same way as Haskell's equality operators using
 @IS [NOT] DISTINCT FROM@.
-}
instance {-# OVERLAPPING #-} DatabaseEq s (Expr s (Maybe a)) where
  (==.) = unsafeBinaryOperator "IS NOT DISTINCT FROM"
  (/=.) = unsafeBinaryOperator "IS DISTINCT FROM"

-- | Pointwise equality
instance
  ( HKD.ConstraintsB (HKD table)
  , HKD.TraversableB (HKD table)
  , Barbie.ProductB (HKD table)
  , Barbie.AllBF (DatabaseEq s) (Expr s) (HKD table)
  ) =>
  DatabaseEq s (HKD table (Expr s))
  where
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
count (Expr e) = AggregateExpr $ "COUNT(" <> e <> ")"

countAll :: AggregateExpr s Int
countAll = AggregateExpr "COUNT(*)"

average :: Num a => Expr s a -> AggregateExpr s (Maybe a)
average (Expr e) = AggregateExpr $ "AVG(" <> e <> ")"

all_ :: Expr s Bool -> AggregateExpr s Bool
all_ (Expr e) = AggregateExpr $ "BOOL_AND(" <> e <> ")"

or_ :: Expr s Bool -> AggregateExpr s Bool
or_ (Expr e) = AggregateExpr $ "BOOL_OR(" <> e <> ")"

max_ :: Expr s a -> AggregateExpr s (Maybe a)
max_ (Expr e) = AggregateExpr $ "MAX(" <> e <> ")"

min_ :: Expr s a -> AggregateExpr s (Maybe a)
min_ (Expr e) = AggregateExpr $ "MIN(" <> e <> ")"

sum_ :: (Num a, Num b) => Expr s a -> AggregateExpr s (Maybe b)
sum_ (Expr e) = AggregateExpr $ "SUM(" <> e <> ")"

arrayAggregate :: DatabaseType a => Expr s a -> AggregateExpr s [a]
arrayAggregate e = AggregateExpr $ "ARRAY_AGG(" <> arrayElement e <> ")"
-------------------------------------------------------------------------------

-- * Arrays

array :: forall s a. DatabaseType a => [Expr s a] -> Expr s [a]
array exprs =
  Expr $
    "ARRAY[" <> mconcat (intersperse ", " $ map arrayElement exprs) <> "]::" <> typeName @[a]

instance Semigroup (Expr s [a]) where
  (<>) = unsafeBinaryOperator "||"

-- | Array contains
(@>) :: Expr s [a] -> Expr s [a] -> Expr s Bool
(@>) = unsafeBinaryOperator "@>"

-- | Array is contained by
(<@) :: Expr s [a] -> Expr s [a] -> Expr s Bool
(<@) = unsafeBinaryOperator "<@"

-- | Arrays overlap (have elements in common). Postgres @&&@ operator.
overlap :: Expr s [a] -> Expr s [a] -> Expr s Bool
overlap = unsafeBinaryOperator "&&"

arrayLength :: Expr s [a] -> Expr s Int
arrayLength (Expr e) = Expr $ "array_length(" <> e <> ", 1)"
-------------------------------------------------------------------------------

-- * Rows

row :: HKD.TraversableB (HKD table) => HKD table (Expr s) -> Expr s table
row table =
  Expr $
    "ROW("
      <> mconcat (intersperse ", " $ Barbie.bfoldMap (\(Expr e) -> [e]) table)
      <> ")"
-------------------------------------------------------------------------------

-- * Going from Haskell types to database types and back

class DatabaseType a where
  value :: a -> Expr s a
  typeName :: Raw.SQL
  decoder :: Decoder a

  arrayElement :: Expr s a -> Raw.SQL
  arrayElement = coerce

  arrayTypeName :: Raw.SQL
  arrayTypeName = typeName @a <> "[]"

  arrayElementDecoder :: Decoding.Array a
  arrayElementDecoder = case decoder of
    Decoder d -> Decoding.valueArray d
    NullableDecoder d -> Decoding.nullableValueArray d

data Decoder a where
  Decoder :: Decoding.Value a -> Decoder a
  NullableDecoder :: Decoding.Value a -> Decoder (Maybe a)

-- | Arrays
instance DatabaseType a => DatabaseType [a] where
  value = array . map value
  typeName = arrayTypeName @a
  decoder = Decoder $ Decoding.array $ Decoding.dimensionArray replicateM arrayElementDecoder
  arrayElement a = "ROW(" <> coerce a <> ")"

-- | Rows
instance
  {-# OVERLAPPABLE #-}
  (Generic table, HKD.Construct Identity table, HKD.Construct Decoding.Composite table, HKD.ConstraintsB (HKD table), HKD.TraversableB (HKD table), Barbie.AllB DatabaseType (HKD table), HKD.Tuple (Const ()) table ()) =>
  DatabaseType table
  where
  value table =
    row $ Barbie.bmapC @DatabaseType (\(Identity field) -> value field) $ HKD.deconstruct table
  typeName = "record"
  decoder =
    Decoder $
      Decoding.composite $
        HKD.construct $
          Barbie.bmapC
            @DatabaseType
            ( \(Const ()) -> case decoder of
                Decoder d -> Decoding.valueComposite d
                NullableDecoder d -> Decoding.nullableValueComposite d
            )
            mempty

type family NonNestedMaybe a :: Constraint where
  NonNestedMaybe (Maybe a) =
    ( TypeError
        ( 'Text "Attempt to use a nested ‘Maybe’ as a database type:"
            ':<>: 'ShowType (Maybe (Maybe a))
            ':<>: 'Text "Since Woobat maps ‘Maybe’ types to nullable database types and there is only one null, nesting is not supported."
        )
    , Impossible
    )
  NonNestedMaybe _ = ()

-- | Nullable types
instance (NonNestedMaybe a, DatabaseType a) => DatabaseType (Maybe a) where
  value Nothing = Expr $ Raw.nullParam <> "::" <> typeName @a
  value (Just a) = coerce $ value a
  typeName = typeName @a
  decoder = case decoder of
    Decoder d -> NullableDecoder d
    NullableDecoder _ -> impossible

-- | @boolean@
instance DatabaseType Bool where
  value = param Encoding.bool
  typeName = "boolean"
  decoder = Decoder Decoding.bool

-- | @integer@
instance DatabaseType Int where
  value = param $ Encoding.int4_int32 . fromIntegral
  typeName = "integer"
  decoder = Decoder Decoding.int

-- | @int2@
instance DatabaseType Int16 where
  value = param Encoding.int2_int16
  typeName = "int2"
  decoder = Decoder Decoding.int

-- | @int4@
instance DatabaseType Int32 where
  value = param Encoding.int4_int32
  typeName = "int4"
  decoder = Decoder Decoding.int

-- | @int8@
instance DatabaseType Int64 where
  value = param Encoding.int8_int64
  typeName = "int8"
  decoder = Decoder Decoding.int

-- | @int2@
instance DatabaseType Word16 where
  value = param Encoding.int2_word16
  typeName = "int2"
  decoder = Decoder Decoding.int

-- | @int4@
instance DatabaseType Word32 where
  value = param Encoding.int4_word32
  typeName = "int4"
  decoder = Decoder Decoding.int

-- | @int8@
instance DatabaseType Word64 where
  value = param Encoding.int8_word64
  typeName = "int8"
  decoder = Decoder Decoding.int

-- | @float4@
instance DatabaseType Float where
  value = param Encoding.float4
  typeName = "float4"
  decoder = Decoder Decoding.float4

-- | @float8@
instance DatabaseType Double where
  value = param Encoding.float8
  typeName = "float8"
  decoder = Decoder Decoding.float8

-- | @numeric@
instance DatabaseType Scientific where
  value = param Encoding.numeric
  typeName = "numeric"
  decoder = Decoder Decoding.numeric

-- | @uuid@
instance DatabaseType UUID where
  value = param Encoding.uuid
  typeName = "uuid"
  decoder = Decoder Decoding.uuid

-- | @character@
instance DatabaseType Char where
  value = param Encoding.char_utf8
  typeName = "character"
  decoder = Decoder Decoding.char

-- | @text@
instance DatabaseType Text where
  value = param Encoding.text_strict
  typeName = "text"
  decoder = Decoder Decoding.text_strict

-- | @text@
instance DatabaseType Lazy.Text where
  value = param Encoding.text_lazy
  typeName = "text"
  decoder = Decoder Decoding.text_lazy

-- | @bytea@
instance DatabaseType ByteString where
  value = param Encoding.bytea_strict
  typeName = "bytea"
  decoder = Decoder Decoding.bytea_strict

-- | @bytea@
instance DatabaseType Lazy.ByteString where
  value = param Encoding.bytea_lazy
  typeName = "bytea"
  decoder = Decoder Decoding.bytea_lazy

-- | @date@
instance DatabaseType Day where
  value = param Encoding.date
  typeName = "date"
  decoder = Decoder Decoding.date

-- | @time@
instance DatabaseType TimeOfDay where
  value = param Encoding.time_int
  typeName = "time"
  decoder = Decoder Decoding.time_int

-- | @timetz@
instance DatabaseType (TimeOfDay, TimeZone) where
  value = param Encoding.timetz_int
  typeName = "timetz"
  decoder = Decoder Decoding.timetz_int

-- | @timestamp@
instance DatabaseType LocalTime where
  value = param Encoding.timestamp_int
  typeName = "timestamp"
  decoder = Decoder Decoding.timestamp_int

-- | @timestamptz@
instance DatabaseType UTCTime where
  value = param Encoding.timestamptz_int
  typeName = "timestamptz"
  decoder = Decoder Decoding.timestamptz_int

-- | @interval@
instance DatabaseType DiffTime where
  value = param Encoding.interval_int
  typeName = "interval"
  decoder = Decoder Decoding.interval_int

-------------------------------------------------------------------------------

-- * Low-level utilities

param :: forall s a. DatabaseType a => (a -> Encoding) -> a -> Expr s a
param encoding a =
  Expr $ Raw.param (Builder.builderBytes $ encoding a) <> "::" <> typeName @a

unsafeBinaryOperator :: Scope.Same s t => Raw.SQL -> Expr s a -> Expr t b -> Expr s c
unsafeBinaryOperator name (Expr x) (Expr y) = Expr $ "(" <> x <> " " <> name <> " " <> y <> ")"

class Impossible where
  impossible :: a
