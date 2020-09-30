{-# language AllowAmbiguousTypes #-}
{-# language DeriveFoldable #-}
{-# language DeriveFunctor #-}
{-# language DeriveTraversable #-}
{-# language FlexibleInstances #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language OverloadedStrings #-}
{-# language ScopedTypeVariables #-}
{-# language TypeApplications #-}
module Database.Woobat.Raw where

import ByteString.StrictBuilder (Builder)
import qualified ByteString.StrictBuilder as Builder
import Control.Monad
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as Lazy
import Data.Int
import Data.Scientific
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Data.String
import Data.Text (Text)
import qualified Data.Text.Lazy as Lazy
import Data.Time (Day, TimeOfDay, LocalTime, UTCTime, DiffTime, TimeZone)
import Data.UUID.Types (UUID)
import Data.Word
import qualified PostgreSQL.Binary.Encoding as Encoding

newtype SQL = SQL (Seq SQLFragment)
  deriving (Show)

data SQLFragment
  = Code !Builder
  | Param !ByteString
  | NullParam
  deriving Show

instance IsString SQLFragment where
  fromString = Code . fromString

instance IsString SQL where
  fromString = SQL . pure . fromString

instance Semigroup SQL where
  SQL (codes1 Seq.:|> Code code2) <> SQL (Code code3 Seq.:<| codes4) =
    SQL $ (codes1 Seq.:|> Code (code2 <> code3)) <> codes4
  SQL codes1 <> SQL codes2 = SQL $ codes1 <> codes2

instance Monoid SQL where
  mempty = SQL mempty

code :: ByteString -> SQL
code = SQL . pure . Code . Builder.bytes

param :: ByteString -> SQL
param = SQL . pure . Param

nullParam :: SQL
nullParam = SQL $ pure NullParam

-------------------------------------------------------------------------------

data Tsil a = Empty | Tsil a :> a
  deriving (Eq, Ord, Show, Functor, Traversable, Foldable)

instance Semigroup (Tsil a) where
  xs <> Empty = xs
  xs <> ys :> y = (xs <> ys) :> y

instance Monoid (Tsil a) where
  mempty = Empty

instance Applicative Tsil where
  pure = (Empty :>)
  (<*>) = ap

instance Monad Tsil where
  return = pure
  Empty >>= _ = Empty
  xs :> x >>= f = (xs >>= f) <> f x

data Select = Select
  { from :: !From
  , wheres :: Tsil SQL
  , groupBys :: Tsil SQL
  , orderBys :: Tsil (SQL, Order)
  } deriving Show

data Order = Ascending | Descending
  deriving (Eq, Show)

data From
  = Unit
  | Table !ByteString !ByteString
  | Subquery [(SQL, ByteString)] !Select !ByteString
  | CrossJoin From (Seq From)
  | LeftJoin !From !SQL !From
  deriving (Show)

instance Semigroup Select where
  Select a1 b1 c1 d1 <> Select a2 b2 c2 d2 =
    Select (a1 <> a2) (b1 <> b2) (c1 <> c2) (d1 <> d2)

instance Monoid Select where
  mempty = Select mempty mempty mempty mempty

instance Semigroup From where
  Unit <> from_ = from_
  from_ <> Unit = from_
  CrossJoin from1 froms1 <> CrossJoin from2 froms2 = CrossJoin from1 (froms1 <> pure from2 <> froms2)
  CrossJoin from1 froms <> from2 = CrossJoin from1 (froms Seq.:|> from2)
  from1 <> CrossJoin from2 froms = CrossJoin from1 (from2 Seq.:<| froms)
  from1 <> from2 = CrossJoin from1 (pure from2)

instance Monoid From where
  mempty = Unit

-------------------------------------------------------------------------------

class DatabaseType a where
  value :: a -> SQL
  typeName :: SQL

-- | Nullable types
instance DatabaseType a => DatabaseType (Maybe a) where
  value Nothing = nullParam
  value (Just a) = value a
  typeName = typeName @a

-- | @boolean@
instance DatabaseType Bool where
  value = param . Builder.builderBytes . Encoding.bool
  typeName = "boolean"

-- | @integer@
instance DatabaseType Int where
  value = param . Builder.builderBytes . Encoding.int4_int32 . fromIntegral
  typeName = "integer"

-- | @int2@
instance DatabaseType Int16 where
  value = param . Builder.builderBytes . Encoding.int2_int16
  typeName = "int2"

-- | @int4@
instance DatabaseType Int32 where
  value = param . Builder.builderBytes . Encoding.int4_int32
  typeName = "int4"

-- | @int8@
instance DatabaseType Int64 where
  value = param . Builder.builderBytes . Encoding.int8_int64
  typeName = "int8"

-- | @int2@
instance DatabaseType Word16 where
  value = param . Builder.builderBytes . Encoding.int2_word16
  typeName = "int2"

-- | @int4@
instance DatabaseType Word32 where
  value = param . Builder.builderBytes . Encoding.int4_word32
  typeName = "int4"

-- | @int8@
instance DatabaseType Word64 where
  value = param . Builder.builderBytes . Encoding.int8_word64
  typeName = "int8"

-- | @float4@
instance DatabaseType Float where
  value = param . Builder.builderBytes . Encoding.float4
  typeName = "float4"

-- | @float8@
instance DatabaseType Double where
  value = param . Builder.builderBytes . Encoding.float8
  typeName = "float8"

-- | @numeric@
instance DatabaseType Scientific where
  value = param . Builder.builderBytes . Encoding.numeric
  typeName = "numeric"

-- | @uuid@
instance DatabaseType UUID where
  value = param . Builder.builderBytes . Encoding.uuid
  typeName = "uuid"

-- | @character@
instance DatabaseType Char where
  value = param . Builder.builderBytes . Encoding.char_utf8
  typeName = "character"

-- | @text@
instance DatabaseType Text where
  value = param . Builder.builderBytes . Encoding.text_strict
  typeName = "text"

-- | @text@
instance DatabaseType Lazy.Text where
  value = param . Builder.builderBytes . Encoding.text_lazy
  typeName = "text"

-- | @bytea@
instance DatabaseType ByteString where
  value = param . Builder.builderBytes . Encoding.bytea_strict
  typeName = "bytea"

-- | @bytea@
instance DatabaseType Lazy.ByteString where
  value = param . Builder.builderBytes . Encoding.bytea_lazy
  typeName = "bytea"

-- | @date@
instance DatabaseType Day where
  value = param . Builder.builderBytes . Encoding.date
  typeName = "date"

-- | @time@
instance DatabaseType TimeOfDay where
  value = param . Builder.builderBytes . Encoding.time_int
  typeName = "time"

-- | @timetz@
instance DatabaseType (TimeOfDay, TimeZone) where
  value = param . Builder.builderBytes . Encoding.timetz_int
  typeName = "timetz"

-- | @timestamp@
instance DatabaseType LocalTime where
  value = param . Builder.builderBytes . Encoding.timestamp_int
  typeName = "timestamp"

-- | @timestamptz@
instance DatabaseType UTCTime where
  value = param . Builder.builderBytes . Encoding.timestamptz_int
  typeName = "timestamptz"

-- | @interval@
instance DatabaseType DiffTime where
  value = param . Builder.builderBytes . Encoding.interval_int
  typeName = "interval"
