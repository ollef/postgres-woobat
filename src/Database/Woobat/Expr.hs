{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module Database.Woobat.Expr (
  module Database.Woobat.Expr.Types,
  module Database.Woobat.Expr,
) where

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
import Data.List.NonEmpty (NonEmpty)
import Data.Scientific
import Data.String (IsString, fromString)
import Data.Text (Text)
import qualified Data.Text.Lazy as Lazy
import Data.Time (Day, DiffTime, LocalTime, TimeOfDay, TimeZone, UTCTime)
import Data.UUID.Types (UUID)
import Database.Woobat.Barbie
import Database.Woobat.Compiler
import Database.Woobat.Expr.Types
import qualified Database.Woobat.Raw as Raw
import qualified Database.Woobat.Scope as Scope
import Database.Woobat.Select.Builder
import GHC.Generics
import GHC.TypeLits (ErrorMessage (..), TypeError)
import qualified PostgreSQL.Binary.Decoding as Decoding
import PostgreSQL.Binary.Encoding (Encoding)
import qualified PostgreSQL.Binary.Encoding as Encoding

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
  signum (Expr a) = Expr $ "SIGN(" <> a <> ")::" <> typeName @a

-- | @%@
mod_ :: Num a => Expr s a -> Expr s a -> Expr s a
mod_ = unsafeBinaryOperator "%"

instance {-# OVERLAPPABLE #-} (Integral a, DatabaseType a) => Fractional (Expr s a) where
  fromRational = value . (truncate :: Double -> a) . fromRational
  (/) = unsafeBinaryOperator "/"

instance Fractional (Expr s Float) where
  fromRational = value . fromRational
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
if_ branches (Expr def) =
  Expr $ "(CASE " <> mconcat ["WHEN " <> cond <> " THEN " <> branch <> " " | (Expr cond, Expr branch) <- branches] <> "ELSE " <> def <> " END)"

-------------------------------------------------------------------------------

-- * Booleans

true :: Expr s Bool
true = value True

false :: Expr s Bool
false = value False

-- | @NOT(x)@
not_ :: Expr s Bool -> Expr s Bool
not_ (Expr e) = Expr $ "NOT(" <> e <> ")"

ifThenElse :: (Scope.Same s t, Scope.Same t u) => Expr s Bool -> Expr t a -> Expr u a -> Expr s a
ifThenElse cond t f =
  if_ [(cond, t)] f

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

-- @GREATEST(xs)@
maximum_ :: NonEmpty (Expr s a) -> Expr s a
maximum_ args = Expr $ "GREATEST(" <> Raw.separateBy ", " (coerce <$> toList args) <> ")"

-- @LEAST(xs)@
minimum_ :: NonEmpty (Expr s a) -> Expr s a
minimum_ args = Expr $ "LEAST(" <> Raw.separateBy ", " (coerce <$> toList args) <> ")"
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

-- | @MAX(x)@
max_ :: Expr s a -> AggregateExpr s (Maybe a)
max_ (Expr e) = AggregateExpr $ "MAX(" <> e <> ")"

-- | @MIN(x)@
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
    "ARRAY[" <> Raw.separateBy ", " (coerce <$> exprs) <> "]::" <> typeName @[a]

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
arrayLength (Expr e) = Expr $ "COALESCE(ARRAY_LENGTH(" <> e <> ", 1), 0)"

instance (NonNestedArray a, DatabaseType a) => DatabaseType [a] where
  typeName = typeName @a <> "[]"
  encode as = "ARRAY[" <> Raw.separateBy ", " (coerce . value <$> as) <> "]"
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
  NonNestedArray (Maybe a) = NonNestedArray a
  NonNestedArray _ = ()

-------------------------------------------------------------------------------

-- * Rows

record ::
  ( HKD.TraversableB (HKD row)
  , HKD.ConstraintsB (HKD row)
  , HKD.AllB DatabaseType (HKD row)
  , HKD.Construct Identity row
  , Generic row
  ) =>
  row ->
  HKD row (Expr s)
record =
  Barbie.bmapC @DatabaseType (\(Identity a) -> value a) . HKD.deconstruct

type Row row = RowF row Identity
newtype RowF row (f :: * -> *) = Row (row f)

instance HKD.FunctorB row => HKD.FunctorB (RowF row) where
  bmap f (Row row_) = Row $ HKD.bmap f row_

instance HKD.TraversableB row => HKD.TraversableB (RowF row) where
  btraverse f (Row row_) = Row <$> HKD.btraverse f row_

instance HKD.TraversableB row => Barbie f (RowF row f) where
  type ToBarbie f (RowF row f) = row
  type FromBarbie f (RowF row f) g = row g
  toBarbie (Row x) = x
  fromBarbie = id

pureRow :: HKD.Construct Identity row => row -> Row (HKD row)
pureRow = Row . HKD.deconstruct

deriving instance Eq (row Identity) => Eq (Row row)
deriving instance Ord (row Identity) => Ord (Row row)
deriving instance Show (row Identity) => Show (Row row)

row :: forall s row. Barbie (Expr s) row => row -> Expr s (Row (ToBarbie (Expr s) row))
row r = do
  let barbieRow :: ToBarbie (Expr s) row (Expr s)
      barbieRow = toBarbie r
  hkdRow barbieRow

fromJSONRow ::
  ( HKD.AllB FromJSON row
  , HKD.TraversableB row
  , HKD.ConstraintsB row
  , Monoid (row (Const ()))
  ) =>
  Expr s (JSONB (Row row)) ->
  row (Expr s)
fromJSONRow (Expr json) =
  flip evalState 1 $ Barbie.btraverseC @FromJSON go mempty
  where
    go :: forall s x. FromJSON x => Const () x -> State Int (Expr s x)
    go (Const ()) = do
      i <- get
      put $! i + 1
      return $ fromJSON $ Expr $ "(" <> json <> "->'f" <> fromString (show i) <> "')"

instance
  ( HKD.AllB DatabaseType row
  , HKD.TraversableB row
  , HKD.ConstraintsB row
  , Monoid (row (Const ()))
  ) =>
  DatabaseType (Row row)
  where
  typeName = "record"
  value (Row r) = do
    let values = Barbie.bmapC @DatabaseType (\(Identity field) -> value field) r
    hkdRow values
  decoder =
    Decoder $
      Decoding.composite $
        Row
          <$> Barbie.btraverseC
            @DatabaseType
            ( \(Const ()) -> case decoder of
                Decoder d -> Identity <$> Decoding.valueComposite d
                NullableDecoder d -> Identity <$> Decoding.nullableValueComposite d
            )
            mempty

instance
  ( HKD.AllB DatabaseType row
  , HKD.AllB FromJSON row
  , HKD.TraversableB row
  , HKD.ConstraintsB row
  , Monoid (row (Const ()))
  ) =>
  FromJSON (Row row)
  where
  fromJSON = hkdRow . fromJSONRow

-------------------------------------------------------------------------------

-- * JSON

newtype JSONB a = JSONB Aeson.Value

class DatabaseType a => FromJSON a where
  fromJSON :: Expr s (JSONB a) -> Expr s a
  fromJSON (Expr e) = Expr $ e <> "::" <> typeName @Text <> "::" <> typeName @a

instance DatabaseType (JSONB a) where
  typeName = "jsonb"
  encode (JSONB json) =
    Raw.paramExpr (Builder.builderBytes $ Encoding.jsonb_ast json)
  decoder =
    Decoder $ JSONB <$> Decoding.jsonb_ast

instance FromJSON (JSONB a) where
  fromJSON = coerce

toJSONB :: forall s a. DatabaseType a => Expr s a -> Expr s (JSONB a)
toJSONB (Expr e) = case decoder @a of
  Decoder _ ->
    Expr $ "TO_JSONB(" <> e <> ")"
  NullableDecoder _ ->
    -- The row here means that we get a non-null JSONB containing null for a null input instead of a null JSONB
    Expr $ "(TO_JSONB(ROW(" <> e <> "))->'f1')"
-------------------------------------------------------------------------------

-- * Nullable types

-- | @null@
nothing :: forall s a. DatabaseType a => Expr s (Maybe a)
nothing = Expr $ "null::" <> typeName @a

just :: (NonNestedMaybe a, DatabaseType a) => Expr s a -> Expr s (Maybe a)
just = coerce

-- | @x IS NOT DISTINCT FROM null@
isNothing_ :: DatabaseType a => Expr s (Maybe a) -> Expr s Bool
isNothing_ e = e ==. nothing

-- | @x IS DISTINCT FROM null@
isJust_ :: DatabaseType a => Expr s (Maybe a) -> Expr s Bool
isJust_ e = e /=. nothing

maybe_ :: (NonNestedMaybe a, DatabaseType a) => Expr s b -> (Expr s a -> Expr s b) -> Expr s (Maybe a) -> Expr s b
maybe_ def f m = ifThenElse (isNothing_ m) def (f $ coerce m)

-- | @COALESCE(x, def)@
fromMaybe_ :: NonNestedMaybe a => Expr s a -> Expr s (Maybe a) -> Expr s a
fromMaybe_ (Expr def) (Expr m) = Expr $ "COALESCE(" <> m <> ", " <> def <> ")"

instance (NonNestedMaybe a, DatabaseType a) => DatabaseType (Maybe a) where
  typeName = typeName @a
  encode Nothing = "null"
  encode (Just a) = encode a
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

-- * Subqueries

-- | @EXISTS(s)@
exists :: Select s a -> Expr s Bool
exists select = Expr $
  Raw.Expr $ \usedNames_ -> do
    let (_, st) = run usedNames_ select
    "EXISTS(" <> compileSelect [] (rawSelect st) <> ")"

-------------------------------------------------------------------------------

-- * Going from Haskell types to database types and back

-- | Types with a corresponding database type
class DatabaseType a where
  value :: a -> Expr s a
  value a = Expr $ encode a <> "::" <> typeName @a
  typeName :: Raw.Expr
  encode :: a -> Raw.Expr
  encode = coerce . value
  decoder :: Decoder a
  {-# MINIMAL (value | encode), typeName, decoder #-}

data Decoder a where
  Decoder :: Decoding.Value a -> Decoder a
  NullableDecoder :: Decoding.Value a -> Decoder (Maybe a)

mapDecoder :: NonNestedMaybe a => (a -> b) -> Decoder a -> Decoder b
mapDecoder f (Decoder d) = Decoder $ f <$> d
mapDecoder _ (NullableDecoder _) = impossible

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
  decoder = Decoder $ (fromIntegral :: Int32 -> Int) <$> Decoding.int

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

param :: forall a. (a -> Encoding) -> a -> Raw.Expr
param encoding =
  Raw.paramExpr . Builder.builderBytes . encoding

unsafeBinaryOperator :: Scope.Same s t => Raw.Expr -> Expr s a -> Expr t b -> Expr s c
unsafeBinaryOperator name (Expr x) (Expr y) = Expr $ "(" <> x <> " " <> name <> " " <> y <> ")"

unsafeCastFromJSONString :: forall s a. DatabaseType a => Expr s (JSONB a) -> Expr s a
unsafeCastFromJSONString (Expr json) = Expr $ "(" <> json <> " #>> '{}')::" <> typeName @a

class Impossible where
  impossible :: a

hkdRow :: HKD.TraversableB row => row (Expr s) -> Expr s (Row row)
hkdRow r = do
  Expr $ "ROW(" <> Raw.separateBy ", " (Barbie.bfoldMap (\(Expr e) -> [e]) r) <> ")"
