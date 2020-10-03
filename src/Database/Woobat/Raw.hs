{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.Woobat.Raw where

import ByteString.StrictBuilder (Builder)
import qualified ByteString.StrictBuilder as Builder
import Control.Monad
import Data.ByteString (ByteString)
import Data.Foldable
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Data.String

newtype SQL = SQL (Seq SQLFragment)
  deriving (Show)

separateCodeAndParams :: SQL -> (ByteString, [Maybe ByteString])
separateCodeAndParams (SQL fragments) = do
  let (codeBuilder, params, _) = foldl' go (mempty, mempty, 1 :: Int) fragments
  (Builder.builderBytes codeBuilder, toList params)
  where
    go (!codeBuilder, params, !paramNumber) fragment =
      case fragment of
        Code c -> (codeBuilder <> c, params, paramNumber)
        Param p -> (codeBuilder <> "$" <> Builder.asciiIntegral paramNumber, params :> Just p, paramNumber + 1)
        NullParam -> (codeBuilder <> "$" <> Builder.asciiIntegral paramNumber, params :> Nothing, paramNumber + 1)

data SQLFragment
  = Code !Builder
  | Param !ByteString
  | NullParam
  deriving (Show)

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
  deriving (Eq, Ord, Show, Functor, Traversable)

instance Foldable Tsil where
  foldMap _ Empty = mempty
  foldMap f (xs :> x) = foldMap f xs `mappend` f x

  toList = reverse . go
    where
      go Empty = []
      go (xs :> x) = x : go xs

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
  }
  deriving (Show)

data Order = Ascending | Descending
  deriving (Eq, Show)

data From
  = Unit
  | Table !ByteString !ByteString
  | Set !SQL !ByteString
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
