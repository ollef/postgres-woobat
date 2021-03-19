{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
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

import qualified Barbies
import qualified Barbies.Constraints as Barbies
import qualified ByteString.StrictBuilder as Builder
import Control.Monad
import Control.Monad.State
import qualified Data.Aeson as Aeson
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
import Database.Woobat.Expr.Types
import Database.Woobat.Query.Monad
import qualified Database.Woobat.Raw as Raw
import Database.Woobat.Select.Builder
import GHC.Generics
import GHC.TypeLits (ErrorMessage (..), TypeError)
import qualified PostgreSQL.Binary.Decoding as Decoding
import PostgreSQL.Binary.Encoding (Encoding)
import qualified PostgreSQL.Binary.Encoding as Encoding

-------------------------------------------------------------------------------

-- * Strings

instance (IsString a, DatabaseType a) => IsString (Expr a) where
  fromString = value . fromString

instance Semigroup (Expr Text) where
  (<>) = unsafeBinaryOperator "||"

instance (DatabaseType a, Semigroup (Expr a), Monoid a) => Monoid (Expr a) where
  mempty = value mempty

-------------------------------------------------------------------------------

-- * Numerics

instance (Num a, DatabaseType a) => Num (Expr a) where
  fromInteger = value . fromInteger
  (+) = unsafeBinaryOperator "+"
  (-) = unsafeBinaryOperator "-"
  (*) = unsafeBinaryOperator "*"
  abs (Expr a) = Expr $ "ABS(" <> a <> ")"
  signum (Expr a) = Expr $ "SIGN(" <> a <> ")::" <> typeName @a

-- | @%@
mod_ :: Num a => Expr a -> Expr a -> Expr a
mod_ = unsafeBinaryOperator "%"

instance {-# OVERLAPPABLE #-} (Integral a, DatabaseType a) => Fractional (Expr a) where
  fromRational = value . (truncate :: Double -> a) . fromRational
  (/) = unsafeBinaryOperator "/"

instance Fractional (Expr Float) where
  fromRational = value . fromRational
  (/) = unsafeBinaryOperator "/"

instance Fractional (Expr Double) where
  fromRational = value . fromRational
  (/) = unsafeBinaryOperator "/"

-------------------------------------------------------------------------------

-- * Equality

class DatabaseEq a where
  (==.) :: a -> a -> Expr Bool
  (/=.) :: a -> a -> Expr Bool
  infix 4 ==., /=.

-- | Handles nulls the same way as Haskell's equality operators using
-- @IS [NOT] DISTINCT FROM@.
instance DatabaseEq (Expr a) where
  (==.) = unsafeBinaryOperator "IS NOT DISTINCT FROM"
  (/=.) = unsafeBinaryOperator "IS DISTINCT FROM"

-- | Pointwise equality
instance
  ( Barbies.ConstraintsB table
  , Barbies.TraversableB table
  , Barbies.ApplicativeB table
  , Barbies.AllBF DatabaseEq Expr table
  ) =>
  DatabaseEq (table Expr)
  where
  table1 ==. table2 =
    foldr_ (&&.) true $
      Barbies.bfoldMap (\(Const e) -> [e]) $
        Barbies.bmapC @(Barbies.ClassF DatabaseEq Expr) (\(Pair x y) -> Const $ x ==. y) $
          Barbies.bzip table1 table2
    where
      foldr_ _ b [] = b
      foldr_ f _ as = foldr1 f as

  table1 /=. table2 =
    foldr_ (||.) false $
      Barbies.bfoldMap (\(Const e) -> [e]) $
        Barbies.bmapC @(Barbies.ClassF DatabaseEq Expr) (\(Pair x y) -> Const $ x /=. y) $
          Barbies.bzip table1 table2
    where
      foldr_ _ b [] = b
      foldr_ f _ as = foldr1 f as

-- | @CASE WHEN@
if_ :: [(Expr Bool, Expr a)] -> Expr a -> Expr a
if_ [] def = def
if_ branches (Expr def) =
  Expr $ "(CASE " <> mconcat ["WHEN " <> cond <> " THEN " <> branch <> " " | (Expr cond, Expr branch) <- branches] <> "ELSE " <> def <> " END)"

-------------------------------------------------------------------------------

-- * Booleans

true :: Expr Bool
true = value True

false :: Expr Bool
false = value False

-- | @NOT(x)@
not_ :: Expr Bool -> Expr Bool
not_ (Expr e) = Expr $ "NOT(" <> e <> ")"

ifThenElse :: Expr Bool -> Expr a -> Expr a -> Expr a
ifThenElse cond t f =
  if_ [(cond, t)] f

-- | @AND@
(&&.) :: Expr Bool -> Expr Bool -> Expr Bool
(&&.) = unsafeBinaryOperator "AND"

infixr 3 &&.

-- | @OR@
(||.) :: Expr Bool -> Expr Bool -> Expr Bool
(||.) = unsafeBinaryOperator "OR"

infixr 2 ||.

-------------------------------------------------------------------------------

-- * Comparison operators

(<.) :: Expr a -> Expr a -> Expr Bool
(<.) = unsafeBinaryOperator "<"

(<=.) :: Expr a -> Expr a -> Expr Bool
(<=.) = unsafeBinaryOperator "<="

(>.) :: Expr a -> Expr a -> Expr Bool
(>.) = unsafeBinaryOperator ">"

(>=.) :: Expr a -> Expr a -> Expr Bool
(>=.) = unsafeBinaryOperator ">="

infix 4 <., <=., >., >=.

-- @GREATEST(xs)@
maximum_ :: NonEmpty (Expr a) -> Expr a
maximum_ args = Expr $ "GREATEST(" <> Raw.separateBy ", " (coerce <$> toList args) <> ")"

-- @LEAST(xs)@
minimum_ :: NonEmpty (Expr a) -> Expr a
minimum_ args = Expr $ "LEAST(" <> Raw.separateBy ", " (coerce <$> toList args) <> ")"
-------------------------------------------------------------------------------

-- * Aggregates

count :: Expr a -> AggregateExpr Int
count (Expr e) = AggregateExpr $ "COUNT(" <> e <> ")"

-- | @COUNT(*)@
countAll :: AggregateExpr Int
countAll = AggregateExpr "COUNT(*)"

-- | @AVG(x)@
average :: Num a => Expr a -> AggregateExpr (Maybe (Averaged a))
average (Expr e) = AggregateExpr $ "AVG(" <> e <> ")"

-- | "numeric for any integer-type argument, double precision for a floating-point argument, otherwise the same as the argument data type"
type family Averaged a where
  Averaged Int = Scientific
  Averaged Int16 = Scientific
  Averaged Int32 = Scientific
  Averaged Int64 = Scientific
  Averaged Float = Double
  Averaged a = a

-- | @COALESCE(BOOL_AND(x), TRUE)@
all_ :: Expr Bool -> AggregateExpr Bool
all_ (Expr e) = fromMaybeAggregate true $ AggregateExpr $ "BOOL_AND(" <> e <> ")"

-- | @COALESCE(BOOL_OR(x), FALSE)@
any_ :: Expr Bool -> AggregateExpr Bool
any_ (Expr e) = fromMaybeAggregate false $ AggregateExpr $ "BOOL_OR(" <> e <> ")"

-- | @MAX(x)@
max_ :: Expr a -> AggregateExpr (Maybe a)
max_ (Expr e) = AggregateExpr $ "MAX(" <> e <> ")"

-- | @MIN(x)@
min_ :: Expr a -> AggregateExpr (Maybe a)
min_ (Expr e) = AggregateExpr $ "MIN(" <> e <> ")"

-- | @COALESCE(SUM(x), 0)@
sum_ :: forall a. (Num a, Num (Summed a), NonNestedMaybe (Summed a), DatabaseType (Summed a)) => Expr a -> AggregateExpr (Summed a)
sum_ (Expr e) = fromMaybeAggregate 0 $ AggregateExpr $ "SUM(" <> e <> ")"

-- | "bigint for smallint or int arguments, numeric for bigint arguments, otherwise the same as the argument data type"
type family Summed a where
  Summed Int = Int64
  Summed Int16 = Int64
  Summed Int32 = Int64
  Summed Int64 = Scientific
  Summed a = a

-- | @COALESCE(ARRAY_AGG(x), ARRAY[])@
arrayAggregate :: (NonNestedArray a, DatabaseType a) => Expr a -> AggregateExpr [a]
arrayAggregate (Expr e) = fromMaybeAggregate (array []) $ AggregateExpr $ "ARRAY_AGG(" <> e <> ")"

-- | @JSONB_AGG(x)@
jsonAggregate :: (NonNestedArray a, DatabaseType a) => Expr (JSONB a) -> AggregateExpr (JSONB [a])
jsonAggregate (Expr e) = fromMaybeAggregate (toJSONB $ array []) $ AggregateExpr $ "JSONB_AGG(" <> e <> ")"

-- | @COALESCE(x, def)@
fromMaybeAggregate :: NonNestedMaybe a => Expr a -> AggregateExpr (Maybe a) -> AggregateExpr a
fromMaybeAggregate (Expr def) (AggregateExpr m) = AggregateExpr $ "COALESCE(" <> m <> ", " <> def <> ")"

-------------------------------------------------------------------------------

-- * Arrays

array :: forall a. (NonNestedArray a, DatabaseType a) => [Expr a] -> Expr [a]
array exprs =
  Expr $
    "ARRAY[" <> Raw.separateBy ", " (coerce <$> exprs) <> "]::" <> typeName @[a]

arrayOf :: NonNestedArray a => Select (Expr a) -> Expr [a]
arrayOf select = Expr $
  Raw.Expr $ \usedNames_ -> do
    let (Expr expr, st) = run usedNames_ select
    "ARRAY(" <> Raw.compileSelect [Raw.unExpr expr $ usedNames st] (rawSelect st) <> ")"

instance Semigroup (Expr [a]) where
  (<>) = unsafeBinaryOperator "||"

-- | Array contains
(@>) :: Expr [a] -> Expr [a] -> Expr Bool
(@>) = unsafeBinaryOperator "@>"

-- | Array is contained by
(<@) :: Expr [a] -> Expr [a] -> Expr Bool
(<@) = unsafeBinaryOperator "<@"

-- | Arrays overlap (have elements in common). Postgres @&&@ operator.
overlap :: Expr [a] -> Expr [a] -> Expr Bool
overlap = unsafeBinaryOperator "&&"

arrayLength :: Expr [a] -> Expr Int
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
  fromJSON expr =
    arrayOf $ fromJSON <$> jsonbArrayElements expr

type family NonNestedArray a :: Constraint where
  NonNestedArray [a] =
    ( TypeError
        ( 'Text "Attempt to use a nested list as a database type:"
            ':$$: 'ShowType [[a]]
            ':$$: 'Text "Since Woobat maps lists to Postgres arrays and multidimensional Postgres arrays must have matching dimensions, unlike Haskell, nesting is not supported."
        )
    , Impossible
    )
  NonNestedArray (Maybe a) = NonNestedArray a
  NonNestedArray _ = ()

-------------------------------------------------------------------------------

-- * Rows

record ::
  ( Barbies.TraversableB (HKD row)
  , Barbies.ConstraintsB (HKD row)
  , Barbies.AllB DatabaseType (HKD row)
  , HKD.Construct Identity row
  , Generic row
  ) =>
  row ->
  HKD row Expr
record =
  Barbies.bmapC @DatabaseType (\(Identity a) -> value a) . HKD.deconstruct

pureRow :: HKD.Construct Identity row => row -> Row (HKD row)
pureRow = Row . HKD.deconstruct

row :: forall row. Barbie Expr row => row -> Expr (Row (ToBarbie Expr row))
row r = do
  let barbieRow :: ToBarbie Expr row Expr
      barbieRow = toBarbie r
  hkdRow barbieRow

fromJSONBRow ::
  ( Barbies.AllB FromJSON row
  , Barbies.TraversableB row
  , Barbies.ConstraintsB row
  , Monoid (row (Const ()))
  ) =>
  Expr (JSONB (Row row)) ->
  row Expr
fromJSONBRow (Expr json) =
  flip evalState 1 $ Barbies.btraverseC @FromJSON go mempty
  where
    go :: forall x. FromJSON x => Const () x -> State Int (Expr x)
    go (Const ()) = do
      i <- get
      put $! i + 1
      return $ fromJSON $ Expr $ "(" <> json <> "->'f" <> fromString (show i) <> "')"

instance
  ( Barbies.AllB DatabaseType row
  , Barbies.TraversableB row
  , Barbies.ConstraintsB row
  , Monoid (row (Const ()))
  ) =>
  DatabaseType (Row row)
  where
  typeName = "record"
  value (Row r) = do
    let values = Barbies.bmapC @DatabaseType (\(Identity field) -> value field) r
    hkdRow values
  decoder =
    Decoder $
      Decoding.composite $
        Row
          <$> Barbies.btraverseC
            @DatabaseType
            ( \(Const ()) -> case decoder of
                Decoder d -> Identity <$> Decoding.valueComposite d
                NullableDecoder d -> Identity <$> Decoding.nullableValueComposite d
            )
            mempty

instance
  ( Barbies.AllB DatabaseType row
  , Barbies.AllB FromJSON row
  , Barbies.TraversableB row
  , Barbies.ConstraintsB row
  , Monoid (row (Const ()))
  ) =>
  FromJSON (Row row)
  where
  fromJSON = hkdRow . fromJSONBRow

-------------------------------------------------------------------------------

-- * JSON

newtype JSONB a = JSONB Aeson.Value

class DatabaseType a => FromJSON a where
  fromJSON :: Expr (JSONB a) -> Expr a
  fromJSON (Expr e) = Expr $ e <> "::" <> typeName @Text <> "::" <> typeName @a

instance DatabaseType (JSONB a) where
  typeName = "jsonb"
  encode (JSONB json) =
    Raw.paramExpr (Builder.builderBytes $ Encoding.jsonb_ast json)
  decoder =
    Decoder $ JSONB <$> Decoding.jsonb_ast

instance FromJSON (JSONB a) where
  fromJSON = coerce

toJSONB :: forall a. DatabaseType a => Expr a -> Expr (JSONB a)
toJSONB (Expr e) = case decoder @a of
  Decoder _ ->
    Expr $ "TO_JSONB(" <> e <> ")"
  NullableDecoder _ ->
    -- The row here means that we get a non-null JSONB containing null for a null input instead of a null JSONB
    Expr $ "(TO_JSONB(ROW(" <> e <> "))->'f1')"

jsonbArrayElements :: MonadQuery m => Expr (JSONB [a]) -> m (Expr (JSONB a))
jsonbArrayElements (Expr arr) = do
  returnAlias <- Raw.code <$> freshName "array_element"
  usedNames_ <- getUsedNames
  addFrom $ Raw.Set ("jsonb_array_elements(" <> Raw.unExpr arr usedNames_ <> ")") returnAlias
  pure $ Expr $ Raw.Expr $ const returnAlias
-------------------------------------------------------------------------------

-- * Nullable types

-- | @null@
nothing :: forall a. DatabaseType a => Expr (Maybe a)
nothing = Expr $ "null::" <> typeName @a

just :: (NonNestedMaybe a, DatabaseType a) => Expr a -> Expr (Maybe a)
just = coerce

-- | @x IS NOT DISTINCT FROM null@
isNothing_ :: DatabaseType a => Expr (Maybe a) -> Expr Bool
isNothing_ e = e ==. nothing

-- | @x IS DISTINCT FROM null@
isJust_ :: DatabaseType a => Expr (Maybe a) -> Expr Bool
isJust_ e = e /=. nothing

maybe_ :: (NonNestedMaybe a, DatabaseType a) => Expr b -> (Expr a -> Expr b) -> Expr (Maybe a) -> Expr b
maybe_ def f m = ifThenElse (isNothing_ m) def (f $ coerce m)

-- | @COALESCE(x, def)@
fromMaybe_ :: NonNestedMaybe a => Expr a -> Expr (Maybe a) -> Expr a
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
            ':$$: 'ShowType (Maybe (Maybe a))
            ':$$: 'Text "Since Woobat maps ‘Maybe’ types to nullable database types and there is only one null, nesting is not supported."
        )
    , Impossible
    )
  NonNestedMaybe _ = ()

-------------------------------------------------------------------------------

-- * Subqueries

-- | @EXISTS(s)@
exists :: Select a -> Expr Bool
exists select = Expr $
  Raw.Expr $ \usedNames_ -> do
    let (_, st) = run usedNames_ select
    "EXISTS(" <> Raw.compileSelect [] (rawSelect st) <> ")"

-------------------------------------------------------------------------------

-- * Going from Haskell types to database types and back

-- | Types with a corresponding database type
class DatabaseType a where
  value :: a -> Expr a
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

unsafeBinaryOperator :: Raw.Expr -> Expr a -> Expr b -> Expr c
unsafeBinaryOperator name (Expr x) (Expr y) = Expr $ "(" <> x <> " " <> name <> " " <> y <> ")"

unsafeCastFromJSONString :: forall a. DatabaseType a => Expr (JSONB a) -> Expr a
unsafeCastFromJSONString (Expr json) = Expr $ "(" <> json <> " #>> '{}')::" <> typeName @a

class Impossible where
  impossible :: a

hkdRow :: Barbies.TraversableB row => row Expr -> Expr (Row row)
hkdRow r = do
  Expr $ "ROW(" <> Raw.separateBy ", " (Barbies.bfoldMap (\(Expr e) -> [e]) r) <> ")"
