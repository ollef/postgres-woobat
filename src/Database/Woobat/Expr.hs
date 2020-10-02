{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DefaultSignatures #-}
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
import Control.Monad.State
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

instance (IsString a, Encode a) => IsString (Expr s a) where
  fromString = encode . fromString

instance Semigroup (Expr s Text) where
  (<>) = unsafeBinaryOperator "||"

instance (Encode a, Semigroup (Expr s a), Monoid a) => Monoid (Expr s a) where
  mempty = encode mempty

-------------------------------------------------------------------------------

-- * Numerics

instance (Num a, Encode a) => Num (Expr s a) where
  fromInteger = encode . fromInteger
  (+) = unsafeBinaryOperator "+"
  (-) = unsafeBinaryOperator "-"
  (*) = unsafeBinaryOperator "*"
  abs (Expr a) = Expr $ "ABS(" <> a <> ")"
  signum (Expr a) = Expr $ "SIGN(" <> a <> ")"

mod_ :: Num a => Expr s a -> Expr s a -> Expr s a
mod_ = unsafeBinaryOperator "%"

instance {-# OVERLAPPABLE #-} (Integral a, Encode a) => Fractional (Expr s a) where
  fromRational = encode . (truncate :: Double -> a) . fromRational
  (/) = unsafeBinaryOperator "/"

instance Fractional (Expr s Double) where
  fromRational = encode . fromRational
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
true = encode True

false :: Expr s Bool
false = encode True

not_ :: Expr s Bool -> Expr s Bool
not_ (Expr e) = Expr $ "NOT(" <> e <> ")"

ifThenElse :: (Scope.Same s t, Scope.Same t u) => Expr s Bool -> Expr t a -> Expr u a -> Expr s a
ifThenElse (Expr cond) (Expr t) (Expr f) =
  Expr $ "(CASE WHEN " <> cond <> " THEN " <> t <> " ELSE " <> f <> " END)"

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

arrayAggregate :: ArrayElement a => Expr s a -> AggregateExpr s [a]
arrayAggregate (Expr e) = AggregateExpr $ "ARRAY_AGG(" <> e <> ")"
-------------------------------------------------------------------------------

-- * Arrays

array :: forall s a. ArrayElement a => [Expr s a] -> Expr s [a]
array exprs =
  Expr $
    "ARRAY[" <> mconcat (intersperse ", " $ coerce exprs) <> "]::" <> typeName @[a]

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
  Expr $ "ROW(" <> mconcat (intersperse ", " $ Barbie.bfoldMap (\(Expr e) -> [e]) table) <> ")"
-------------------------------------------------------------------------------

-- * JSON

data JSONB a

instance DatabaseType (JSONB a) where
  typeName = "jsonb"

instance ArrayElement (JSONB a)

toJSONB :: Expr s a -> Expr s (JSONB a)
toJSONB (Expr e) = Expr $ "TO_JSONB(" <> e <> ")"

class DatabaseType a => FromJSON a where
  fromJSON :: Expr s (JSONB a) -> Expr s a
  fromJSON (Expr e) = Expr $ e <> "::" <> typeName @Text <> "::" <> typeName @a

-------------------------------------------------------------------------------

-- * Nullable types
nothing :: forall s a. (NonNestedMaybe a, DatabaseType a) => Expr s (Maybe a)
nothing = Expr $ "null::" <> typeName @a

isNothing_ :: Expr s (Maybe a) -> Expr s Bool
isNothing_ (Expr e) = Expr $ "(" <> e <> " IS NULL)"

isJust_ :: Expr s (Maybe a) -> Expr s Bool
isJust_ (Expr e) = Expr $ "(" <> e <> " IS NOT NULL)"

just :: (NonNestedMaybe a, DatabaseType a) => Expr s a -> Expr s (Maybe a)
just = coerce

maybe_ :: Expr s b -> (Expr s a -> Expr s b) -> Expr s (Maybe a) -> Expr s b
maybe_ def f m = ifThenElse (isNothing_ m) def (f $ coerce m)

fromMaybe_ :: Expr s a -> Expr s (Maybe a) -> Expr s a
fromMaybe_ def = maybe_ def id

instance (NonNestedMaybe a, DatabaseType a) => DatabaseType (Maybe a) where
  typeName = typeName @a

instance (NonNestedMaybe a, Encode a) => Encode (Maybe a) where
  encode Nothing = nothing
  encode (Just a) = just $ encode a

instance (NonNestedMaybe a, Decode a) => Decode (Maybe a) where
  decoder = case decoder of
    Decoder d -> NullableDecoder d
    NullableDecoder _ -> impossible

instance (NonNestedMaybe a, FromJSON a) => FromJSON (Maybe a) where
  fromJSON json =
    ifThenElse (json ==. toJSONB nothing) nothing (just $ fromJSON $ coerce json)

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

-------------------------------------------------------------------------------

-- * Going from Haskell types to database types and back

-- | Types with a corresponding database type
class DatabaseType a where
  typeName :: Raw.SQL

-- | Types that can be encoded to database expressions
class DatabaseType a => Encode a where
  encode :: a -> Expr s a

-- | Types that can be decoded from database results
class DatabaseType a => Decode a where
  decoder :: Decoder a

-- | Types that can be used as array elements
class DatabaseType a => ArrayElement a

data Decoder a where
  Decoder :: Decoding.Value a -> Decoder a
  NullableDecoder :: Decoding.Value a -> Decoder (Maybe a)

-- | Arrays
instance ArrayElement a => DatabaseType [a] where
  typeName = typeName @a <> "[]"

instance (ArrayElement a, Encode a) => Encode [a] where
  encode = array . map encode

instance (ArrayElement a, Decode a) => Decode [a] where
  decoder = Decoder $
    Decoding.array $
      Decoding.dimensionArray
        replicateM
        $ case decoder of
          Decoder d -> Decoding.valueArray d
          NullableDecoder d -> Decoding.nullableValueArray d

instance (ArrayElement a, FromJSON a) => FromJSON [a] where
  fromJSON (Expr json) =
    Expr $
      -- TODO needs fresh names
      "ARRAY(SELECT " <> coerce (fromJSON @a $ Expr "element.value") <> " FROM JSONB_ARRAY_ELEMENTS(" <> json <> ") AS element)"

-- | Rows
instance {-# OVERLAPPABLE #-} (HKD.FunctorB (HKD table)) => DatabaseType table where
  typeName = "record"

instance
  {-# OVERLAPPABLE #-}
  (HKD.Construct Identity table, HKD.ConstraintsB (HKD table), HKD.TraversableB (HKD table), Barbie.AllB Encode (HKD table), HKD.Tuple (Const ()) table ()) =>
  Encode table
  where
  encode table =
    row $ Barbie.bmapC @Encode (\(Identity field) -> encode field) $ HKD.deconstruct table

instance
  {-# OVERLAPPABLE #-}
  (Generic table, HKD.Construct Decoding.Composite table, HKD.ConstraintsB (HKD table), Barbie.AllB Decode (HKD table), HKD.Tuple (Const ()) table ()) =>
  Decode table
  where
  decoder =
    Decoder $
      Decoding.composite $
        HKD.construct $
          Barbie.bmapC
            @Decode
            ( \(Const ()) -> case decoder of
                Decoder d -> Decoding.valueComposite d
                NullableDecoder d -> Decoding.nullableValueComposite d
            )
            mempty

instance {-# OVERLAPPABLE #-} (HKD.FunctorB (HKD table)) => ArrayElement table

instance
  {-# OVERLAPPABLE #-}
  (Generic table, HKD.ConstraintsB (HKD table), HKD.TraversableB (HKD table), HKD.AllB FromJSON (HKD table), HKD.Tuple (Const ()) table ()) =>
  FromJSON table
  where
  fromJSON (Expr json) =
    row $ flip evalState 1 $ Barbie.btraverseC @FromJSON go mempty
    where
      go :: forall s x. FromJSON x => Const () x -> State Int (Expr s x)
      go (Const ()) = do
        i <- get
        put $! i + 1
        return $ fromJSON $ Expr $ json <> "->'f" <> fromString (show i) <> "'"

-- | @boolean@
instance DatabaseType Bool where
  typeName = "boolean"

instance Encode Bool where
  encode = param Encoding.bool

instance Decode Bool where
  decoder = Decoder Decoding.bool

instance FromJSON Bool

-- | @integer@
instance DatabaseType Int where
  typeName = "integer"

instance Encode Int where
  encode = param $ Encoding.int4_int32 . fromIntegral

instance Decode Int where
  decoder = Decoder Decoding.int

instance FromJSON Int

-- | @int2@
instance DatabaseType Int16 where
  typeName = "int2"

instance Encode Int16 where
  encode = param Encoding.int2_int16

instance Decode Int16 where
  decoder = Decoder Decoding.int

instance FromJSON Int16

-- | @int4@
instance DatabaseType Int32 where
  typeName = "int4"

instance Encode Int32 where
  encode = param Encoding.int4_int32

instance Decode Int32 where
  decoder = Decoder Decoding.int

instance FromJSON Int32

-- | @int8@
instance DatabaseType Int64 where
  typeName = "int8"

instance Encode Int64 where
  encode = param Encoding.int8_int64

instance Decode Int64 where
  decoder = Decoder Decoding.int

instance FromJSON Int64

-- | @int2@
instance DatabaseType Word16 where
  typeName = "int2"

instance Encode Word16 where
  encode = param Encoding.int2_word16

instance Decode Word16 where
  decoder = Decoder Decoding.int

instance FromJSON Word16

-- | @int4@
instance DatabaseType Word32 where
  typeName = "int4"

instance Encode Word32 where
  encode = param Encoding.int4_word32

instance Decode Word32 where
  decoder = Decoder Decoding.int

instance FromJSON Word32

-- | @int8@
instance DatabaseType Word64 where
  typeName = "int8"

instance Encode Word64 where
  encode = param Encoding.int8_word64

instance Decode Word64 where
  decoder = Decoder Decoding.int

instance FromJSON Word64

-- | @float4@
instance DatabaseType Float where
  typeName = "float4"

instance Encode Float where
  encode = param Encoding.float4

instance Decode Float where
  decoder = Decoder Decoding.float4

instance FromJSON Float

-- | @float8@
instance DatabaseType Double where
  typeName = "float8"

instance Encode Double where
  encode = param Encoding.float8

instance Decode Double where
  decoder = Decoder Decoding.float8

instance FromJSON Double

-- | @numeric@
instance DatabaseType Scientific where
  typeName = "numeric"

instance Encode Scientific where
  encode = param Encoding.numeric

instance Decode Scientific where
  decoder = Decoder Decoding.numeric

instance FromJSON Scientific

-- | @uuid@
instance DatabaseType UUID where
  typeName = "uuid"

instance Encode UUID where
  encode = param Encoding.uuid

instance Decode UUID where
  decoder = Decoder Decoding.uuid

instance FromJSON UUID where
  fromJSON (Expr json) = Expr $ "(" <> json <> " #>> '{}')::" <> typeName @UUID

-- | @character@
instance DatabaseType Char where
  typeName = "character"

instance Encode Char where
  encode = param Encoding.char_utf8

instance Decode Char where
  decoder = Decoder Decoding.char

instance FromJSON Char where
  fromJSON (Expr json) = Expr $ "(" <> json <> " #>> '{}')::" <> typeName @Char

-- | @text@
instance DatabaseType Text where
  typeName = "text"

instance Encode Text where
  encode = param Encoding.text_strict

instance Decode Text where
  decoder = Decoder Decoding.text_strict

instance FromJSON Text where
  fromJSON (Expr json) = Expr $ "(" <> json <> " #>> '{}')::" <> typeName @Text

-- | @text@
instance DatabaseType Lazy.Text where
  typeName = "text"

instance Encode Lazy.Text where
  encode = param Encoding.text_lazy

instance Decode Lazy.Text where
  decoder = Decoder Decoding.text_lazy

instance FromJSON Lazy.Text where
  fromJSON (Expr json) = Expr $ "(" <> json <> " #>> '{}')::" <> typeName @Lazy.Text

-- | @bytea@
instance DatabaseType ByteString where
  typeName = "bytea"

instance Encode ByteString where
  encode = param Encoding.bytea_strict

instance Decode ByteString where
  decoder = Decoder Decoding.bytea_strict

instance FromJSON ByteString where
  fromJSON (Expr json) = Expr $ "(" <> json <> " #>> '{}')::" <> typeName @ByteString

-- | @bytea@
instance DatabaseType Lazy.ByteString where
  typeName = "bytea"

instance Encode Lazy.ByteString where
  encode = param Encoding.bytea_lazy

instance Decode Lazy.ByteString where
  decoder = Decoder Decoding.bytea_lazy

instance FromJSON Lazy.ByteString where
  fromJSON (Expr json) = Expr $ "(" <> json <> " #>> '{}')::" <> typeName @Lazy.ByteString

-- | @date@
instance DatabaseType Day where
  typeName = "date"

instance Encode Day where
  encode = param Encoding.date

instance Decode Day where
  decoder = Decoder Decoding.date

instance FromJSON Day where
  fromJSON (Expr json) = Expr $ "(" <> json <> " #>> '{}')::" <> typeName @Day

-- | @time@
instance DatabaseType TimeOfDay where
  typeName = "time"

instance Encode TimeOfDay where
  encode = param Encoding.time_int

instance Decode TimeOfDay where
  decoder = Decoder Decoding.time_int

instance FromJSON TimeOfDay where
  fromJSON (Expr json) = Expr $ "(" <> json <> " #>> '{}')::" <> typeName @TimeOfDay

-- | @timetz@
instance DatabaseType (TimeOfDay, TimeZone) where
  typeName = "timetz"

instance Encode (TimeOfDay, TimeZone) where
  encode = param Encoding.timetz_int

instance Decode (TimeOfDay, TimeZone) where
  decoder = Decoder Decoding.timetz_int

instance FromJSON (TimeOfDay, TimeZone) where
  fromJSON (Expr json) = Expr $ "(" <> json <> " #>> '{}')::" <> typeName @(TimeOfDay, TimeZone)

-- | @timestamp@
instance DatabaseType LocalTime where
  typeName = "timestamp"

instance Encode LocalTime where
  encode = param Encoding.timestamp_int

instance Decode LocalTime where
  decoder = Decoder Decoding.timestamp_int

instance FromJSON LocalTime where
  fromJSON (Expr json) = Expr $ "(" <> json <> " #>> '{}')::" <> typeName @LocalTime

-- | @timestamptz@
instance DatabaseType UTCTime where
  typeName = "timestamptz"

instance Encode UTCTime where
  encode = param Encoding.timestamptz_int

instance Decode UTCTime where
  decoder = Decoder Decoding.timestamptz_int

instance FromJSON UTCTime where
  fromJSON (Expr json) = Expr $ "(" <> json <> " #>> '{}')::" <> typeName @UTCTime

-- | @interval@
instance DatabaseType DiffTime where
  typeName = "interval"

instance Encode DiffTime where
  encode = param Encoding.interval_int

instance Decode DiffTime where
  decoder = Decoder Decoding.interval_int

instance FromJSON DiffTime where
  fromJSON (Expr json) = Expr $ "(" <> json <> " #>> '{}')::" <> typeName @DiffTime

-------------------------------------------------------------------------------

-- * Low-level utilities

param :: forall s a. DatabaseType a => (a -> Encoding) -> a -> Expr s a
param encoding a =
  Expr $ Raw.param (Builder.builderBytes $ encoding a) <> "::" <> typeName @a

unsafeBinaryOperator :: Scope.Same s t => Raw.SQL -> Expr s a -> Expr t b -> Expr s c
unsafeBinaryOperator name (Expr x) (Expr y) = Expr $ "(" <> x <> " " <> name <> " " <> y <> ")"

class Impossible where
  impossible :: a
