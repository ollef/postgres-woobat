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
import Control.Monad.State
import qualified Data.Aeson as Aeson
import qualified Data.Barbie as Barbie
import qualified Data.Barbie.Constraints as Barbie
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as Lazy
import Data.Coerce
import Data.Foldable
import Data.Functor.Const
import Data.Functor.Identity
import Data.Functor.Product
import Data.Generic.HKD (HKD)
import qualified Data.Generic.HKD as HKD
import Data.Int
import Data.Kind (Constraint)
import Data.List
import Data.List.NonEmpty (NonEmpty)
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

type NullableExpr s a = NullableF (Expr s) a

newtype NullableF f a = NullableF (f (Nullable a))

type family Nullable a where
  Nullable (Maybe a) = Maybe a
  Nullable a = Maybe a

-------------------------------------------------------------------------------

-- * Strings

instance (IsString a, DatabaseType a) => IsString (Expr s a) where
  fromString = encode . fromString

instance Semigroup (Expr s Text) where
  (<>) = unsafeBinaryOperator "||"

instance (DatabaseType a, Semigroup (Expr s a), Monoid a) => Monoid (Expr s a) where
  mempty = encode mempty

-------------------------------------------------------------------------------

-- * Numerics

instance (Num a, DatabaseType a) => Num (Expr s a) where
  fromInteger = encode . fromInteger
  (+) = unsafeBinaryOperator "+"
  (-) = unsafeBinaryOperator "-"
  (*) = unsafeBinaryOperator "*"
  abs (Expr a) = Expr $ "ABS(" <> a <> ")"
  signum (Expr a) = Expr $ "SIGN(" <> a <> ")"

mod_ :: Num a => Expr s a -> Expr s a -> Expr s a
mod_ = unsafeBinaryOperator "%"

instance {-# OVERLAPPABLE #-} (Integral a, DatabaseType a) => Fractional (Expr s a) where
  fromRational = encode . (truncate :: Double -> a) . fromRational
  (/) = unsafeBinaryOperator "/"

instance Fractional (Expr s Float) where
  fromRational = encode . fromRational
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

-- | Handles nulls the same way as Haskell's equality operators using
-- @IS [NOT] DISTINCT FROM@.
instance DatabaseEq s (Expr s a) where
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

-- | @CASE WHEN@
if_ :: (Scope.Same s t, Scope.Same t u) => [(Expr s Bool, Expr t a)] -> Expr u a -> Expr u a
if_ [] def = def
if_ branches (Expr def) = Expr $ "(CASE " <> mconcat ["WHEN " <> cond <> " THEN " <> branch <> " " | (Expr cond, Expr branch) <- branches] <> "ELSE " <> def <> " END)"

-------------------------------------------------------------------------------

-- * Booleans

true :: Expr s Bool
true = encode True

false :: Expr s Bool
false = encode True

-- | @NOT()@
not_ :: Expr s Bool -> Expr s Bool
not_ (Expr e) = Expr $ "NOT(" <> e <> ")"

ifThenElse :: (Scope.Same s t, Scope.Same t u) => Expr s Bool -> Expr t a -> Expr u a -> Expr s a
ifThenElse cond t f = if_ [(cond, t)] f

-- | @AND@
(&&.) :: Scope.Same s t => Expr s Bool -> Expr t Bool -> Expr s Bool
(&&.) = unsafeBinaryOperator "AND"

infixr 3 &&.

-- | @OR@
(||.) :: Scope.Same s t => Expr s a -> Expr t a -> Expr s Bool
(||.) = unsafeBinaryOperator "OR"

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

-- @GREATEST()@
maximum_ :: NonEmpty (Expr s a) -> Expr s a
maximum_ args = Expr $ "GREATEST(" <> mconcat (intersperse ", " (toList $ coerce <$> args)) <> ")"

-- @LEAST()@
minimum_ :: NonEmpty (Expr s a) -> Expr s a
minimum_ args = Expr $ "LEAST(" <> mconcat (intersperse ", " (toList $ coerce <$> args)) <> ")"
-------------------------------------------------------------------------------

-- * Aggregates

count :: Expr s a -> AggregateExpr s Int
count (Expr e) = AggregateExpr $ "COUNT(" <> e <> ")"

-- | @COUNT(*)@
countAll :: AggregateExpr s Int
countAll = AggregateExpr "COUNT(*)"

-- | @AVG()@
average :: Num a => Expr s a -> AggregateExpr s (Maybe a)
average (Expr e) = AggregateExpr $ "AVG(" <> e <> ")"

-- | @BOOL_AND()@
all_ :: Expr s Bool -> AggregateExpr s Bool
all_ (Expr e) = AggregateExpr $ "BOOL_AND(" <> e <> ")"

-- | @BOOL_OR()@
or_ :: Expr s Bool -> AggregateExpr s Bool
or_ (Expr e) = AggregateExpr $ "BOOL_OR(" <> e <> ")"

-- | @MAX()@
max_ :: Expr s a -> AggregateExpr s (Maybe a)
max_ (Expr e) = AggregateExpr $ "MAX(" <> e <> ")"

-- | @MIN()@
min_ :: Expr s a -> AggregateExpr s (Maybe a)
min_ (Expr e) = AggregateExpr $ "MIN(" <> e <> ")"

-- | @SUM()@
sum_ :: (Num a, Num b) => Expr s a -> AggregateExpr s (Maybe b)
sum_ (Expr e) = AggregateExpr $ "SUM(" <> e <> ")"

-- | @ARRAY_AGG()@
arrayAggregate :: NonNestedArray a => Expr s a -> AggregateExpr s [a]
arrayAggregate (Expr e) = AggregateExpr $ "ARRAY_AGG(" <> e <> ")"

-- | @JSONB_AGG()@
jsonAggregate :: Expr s (JSONB a) -> AggregateExpr s (JSONB [a])
jsonAggregate (Expr e) = AggregateExpr $ "JSONB_AGG(" <> e <> ")"

-------------------------------------------------------------------------------

-- * Arrays

array :: forall s a. (NonNestedArray a, DatabaseType a) => [Expr s a] -> Expr s [a]
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
arrayLength (Expr e) = Expr $ "ARRAY_LENGTH(" <> e <> ", 1)"

instance (NonNestedArray a, DatabaseType a) => DatabaseType [a] where
  typeName = typeName @a <> "[]"
  encode = array . map encode
  decoder = Decoder $
    Decoding.array $
      Decoding.dimensionArray
        replicateM
        $ case decoder of
          Decoder d -> Decoding.valueArray d
          NullableDecoder d -> Decoding.nullableValueArray d

instance (NonNestedArray a, FromJSON a) => FromJSON [a] where
  fromJSON (Expr json) =
    Expr $
      -- TODO needs fresh names
      "ARRAY(SELECT " <> coerce (fromJSON @a $ Expr "element.value") <> " FROM JSONB_ARRAY_ELEMENTS(" <> json <> ") AS element)"

type family NonNestedArray a :: Constraint where
  NonNestedArray [a] =
    ( TypeError
        ( 'Text "Attempt to use a nested list as a database type:"
            ':<>: 'ShowType [[a]]
            ':<>: 'Text "Since Woobat maps lists to Postgres arrays and multidimensional Postgres arrays must have matching dimensions, unlike Haskell, nesting is not supported."
        )
    , Impossible
    )
  NonNestedArray _ = ()

-------------------------------------------------------------------------------

-- * Rows

row :: HKD.TraversableB (HKD table) => HKD table (Expr s) -> Expr s table
row table =
  Expr $ "ROW(" <> mconcat (intersperse ", " $ Barbie.bfoldMap (\(Expr e) -> [e]) table) <> ")"

fromJSONRow ::
  ( Generic table
  , HKD.ConstraintsB (HKD table)
  , HKD.TraversableB (HKD table)
  , Barbie.AllB FromJSON (HKD table)
  , Monoid tuple
  , HKD.Tuple (Const ()) table tuple
  ) =>
  Expr s (JSONB table) ->
  HKD table (Expr s)
fromJSONRow (Expr json) =
  flip evalState 1 $ Barbie.btraverseC @FromJSON go mempty
  where
    go :: forall s x. FromJSON x => Const () x -> State Int (Expr s x)
    go (Const ()) = do
      i <- get
      put $! i + 1
      return $ fromJSON $ Expr $ json <> "->'f" <> fromString (show i) <> "'"

instance
  {-# OVERLAPPABLE #-}
  ( Generic table
  , HKD.Construct Identity table
  , HKD.Construct Decoding.Composite table
  , HKD.ConstraintsB (HKD table)
  , HKD.TraversableB (HKD table)
  , Barbie.AllB DatabaseType (HKD table)
  , Monoid tuple
  , HKD.Tuple (Const ()) table tuple
  ) =>
  DatabaseType table
  where
  typeName = "record"
  encode table =
    row $ Barbie.bmapC @DatabaseType (\(Identity field) -> encode field) $ HKD.deconstruct table
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

instance
  {-# OVERLAPPABLE #-}
  ( Generic table
  , HKD.Construct Identity table
  , HKD.Construct Decoding.Composite table
  , HKD.ConstraintsB (HKD table)
  , HKD.TraversableB (HKD table)
  , Barbie.AllB DatabaseType (HKD table)
  , Barbie.AllB FromJSON (HKD table)
  , Monoid tuple
  , HKD.Tuple (Const ()) table tuple
  ) =>
  FromJSON table
  where
  fromJSON = row . fromJSONRow

-------------------------------------------------------------------------------

-- * JSON

newtype JSONB a = JSONB Aeson.Value

class DatabaseType a => FromJSON a where
  fromJSON :: Expr s (JSONB a) -> Expr s a
  fromJSON (Expr e) = Expr $ e <> "::" <> typeName @Text <> "::" <> typeName @a

instance DatabaseType (JSONB a) where
  typeName = "jsonb"
  encode (JSONB value) =
    Expr $ Raw.param (Builder.builderBytes $ Encoding.jsonb_ast value) <> "::" <> typeName @(JSONB a)
  decoder =
    Decoder $ JSONB <$> Decoding.jsonb_ast

instance FromJSON (JSONB a) where
  fromJSON = coerce

toJSONB :: Expr s a -> Expr s (JSONB a)
toJSONB (Expr e) = Expr $ "TO_JSONB(" <> e <> ")"
-------------------------------------------------------------------------------

-- * Nullable types

-- | @null@
nothing :: forall s a. (NonNestedMaybe a, DatabaseType a) => Expr s (Maybe a)
nothing = Expr $ "null::" <> typeName @a

-- | @IS NULL@
isNothing_ :: Expr s (Maybe a) -> Expr s Bool
isNothing_ (Expr e) = Expr $ "(" <> e <> " IS NULL)"

-- | @IS NOT NULL@
isJust_ :: Expr s (Maybe a) -> Expr s Bool
isJust_ (Expr e) = Expr $ "(" <> e <> " IS NOT NULL)"

just :: (NonNestedMaybe a, DatabaseType a) => Expr s a -> Expr s (Maybe a)
just = coerce

maybe_ :: Expr s b -> (Expr s a -> Expr s b) -> Expr s (Maybe a) -> Expr s b
maybe_ def f m = ifThenElse (isNothing_ m) def (f $ coerce m)

-- | @COALESCE()@
fromMaybe_ :: Expr s a -> Expr s (Maybe a) -> Expr s a
fromMaybe_ (Expr def) (Expr m) = Expr $ "COALESCE(" <> m <> ", " <> def <> ")"

instance (NonNestedMaybe a, DatabaseType a) => DatabaseType (Maybe a) where
  typeName = typeName @a
  encode Nothing = nothing
  encode (Just a) = just $ encode a
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
  encode :: a -> Expr s a
  decoder :: Decoder a

data Decoder a where
  Decoder :: Decoding.Value a -> Decoder a
  NullableDecoder :: Decoding.Value a -> Decoder (Maybe a)

-- | @boolean@
instance DatabaseType Bool where
  typeName = "boolean"
  encode = param Encoding.bool
  decoder = Decoder Decoding.bool

instance FromJSON Bool

-- | @integer@
instance DatabaseType Int where
  typeName = "integer"
  encode = param $ Encoding.int4_int32 . fromIntegral
  decoder = Decoder Decoding.int

instance FromJSON Int

-- | @int2@
instance DatabaseType Int16 where
  typeName = "int2"
  encode = param Encoding.int2_int16
  decoder = Decoder Decoding.int

instance FromJSON Int16

-- | @int4@
instance DatabaseType Int32 where
  typeName = "int4"
  encode = param Encoding.int4_int32
  decoder = Decoder Decoding.int

instance FromJSON Int32

-- | @int8@
instance DatabaseType Int64 where
  typeName = "int8"
  encode = param Encoding.int8_int64
  decoder = Decoder Decoding.int

instance FromJSON Int64

-- | @int2@
instance DatabaseType Word16 where
  typeName = "int2"
  encode = param Encoding.int2_word16
  decoder = Decoder Decoding.int

instance FromJSON Word16

-- | @int4@
instance DatabaseType Word32 where
  typeName = "int4"
  encode = param Encoding.int4_word32
  decoder = Decoder Decoding.int

instance FromJSON Word32

-- | @int8@
instance DatabaseType Word64 where
  typeName = "int8"
  encode = param Encoding.int8_word64
  decoder = Decoder Decoding.int

instance FromJSON Word64

-- | @float4@
instance DatabaseType Float where
  typeName = "float4"
  encode = param Encoding.float4
  decoder = Decoder Decoding.float4

instance FromJSON Float

-- | @float8@
instance DatabaseType Double where
  typeName = "float8"
  encode = param Encoding.float8
  decoder = Decoder Decoding.float8

instance FromJSON Double

-- | @numeric@
instance DatabaseType Scientific where
  typeName = "numeric"
  encode = param Encoding.numeric
  decoder = Decoder Decoding.numeric

instance FromJSON Scientific

-- | @uuid@
instance DatabaseType UUID where
  typeName = "uuid"
  encode = param Encoding.uuid
  decoder = Decoder Decoding.uuid

instance FromJSON UUID where
  fromJSON = unsafeCastFromJSONString

-- | @character@
instance DatabaseType Char where
  typeName = "character"
  encode = param Encoding.char_utf8
  decoder = Decoder Decoding.char

instance FromJSON Char where
  fromJSON = unsafeCastFromJSONString

-- | @text@
instance DatabaseType Text where
  typeName = "text"
  encode = param Encoding.text_strict
  decoder = Decoder Decoding.text_strict

instance FromJSON Text where
  fromJSON = unsafeCastFromJSONString

-- | @text@
instance DatabaseType Lazy.Text where
  typeName = "text"
  encode = param Encoding.text_lazy
  decoder = Decoder Decoding.text_lazy

instance FromJSON Lazy.Text where
  fromJSON = unsafeCastFromJSONString

-- | @bytea@
instance DatabaseType ByteString where
  typeName = "bytea"
  encode = param Encoding.bytea_strict
  decoder = Decoder Decoding.bytea_strict

instance FromJSON ByteString where
  fromJSON = unsafeCastFromJSONString

-- | @bytea@
instance DatabaseType Lazy.ByteString where
  typeName = "bytea"
  encode = param Encoding.bytea_lazy
  decoder = Decoder Decoding.bytea_lazy

instance FromJSON Lazy.ByteString where
  fromJSON = unsafeCastFromJSONString

-- | @date@
instance DatabaseType Day where
  typeName = "date"
  encode = param Encoding.date
  decoder = Decoder Decoding.date

instance FromJSON Day where
  fromJSON = unsafeCastFromJSONString

-- | @time@
instance DatabaseType TimeOfDay where
  typeName = "time"
  encode = param Encoding.time_int
  decoder = Decoder Decoding.time_int

instance FromJSON TimeOfDay where
  fromJSON = unsafeCastFromJSONString

-- | @timetz@
instance DatabaseType (TimeOfDay, TimeZone) where
  typeName = "timetz"
  encode = param Encoding.timetz_int
  decoder = Decoder Decoding.timetz_int

instance FromJSON (TimeOfDay, TimeZone) where
  fromJSON = unsafeCastFromJSONString

-- | @timestamp@
instance DatabaseType LocalTime where
  typeName = "timestamp"
  encode = param Encoding.timestamp_int
  decoder = Decoder Decoding.timestamp_int

instance FromJSON LocalTime where
  fromJSON = unsafeCastFromJSONString

-- | @timestamptz@
instance DatabaseType UTCTime where
  typeName = "timestamptz"
  encode = param Encoding.timestamptz_int
  decoder = Decoder Decoding.timestamptz_int

instance FromJSON UTCTime where
  fromJSON = unsafeCastFromJSONString

-- | @interval@
instance DatabaseType DiffTime where
  typeName = "interval"
  encode = param Encoding.interval_int
  decoder = Decoder Decoding.interval_int

instance FromJSON DiffTime where
  fromJSON = unsafeCastFromJSONString

-------------------------------------------------------------------------------

-- * Low-level utilities

param :: forall s a. DatabaseType a => (a -> Encoding) -> a -> Expr s a
param encoding a =
  Expr $ Raw.param (Builder.builderBytes $ encoding a) <> "::" <> typeName @a

unsafeBinaryOperator :: Scope.Same s t => Raw.SQL -> Expr s a -> Expr t b -> Expr s c
unsafeBinaryOperator name (Expr x) (Expr y) = Expr $ "(" <> x <> " " <> name <> " " <> y <> ")"

unsafeCastFromJSONString :: forall s a. DatabaseType a => Expr s (JSONB a) -> Expr s a
unsafeCastFromJSONString (Expr json) = Expr $ "(" <> json <> " #>> '{}')::" <> typeName @a

class Impossible where
  impossible :: a
