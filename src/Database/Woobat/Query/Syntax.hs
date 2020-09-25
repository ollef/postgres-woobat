{-# language GADTs #-}
{-# language TypeFamilies #-}
module Database.Woobat.Query.Syntax where

import Control.Monad (ap)
import Data.Generic.HKD (HKD)
import Database.Woobat.Expr
import Database.Woobat.Scope
import Database.Woobat.Table

data Query s result where
  Pure :: result -> Query s result
  Select :: Table table -> (Row s table -> Query s result) -> Query s result
  Where :: Expr s Bool -> Query s result -> Query s result
  Order :: Expr s a -> Order -> Query s result -> Query s result
  Distinct :: Query (Inner s) a -> (Outer a -> Query s result) -> Query s result
  InnerJoin :: Query (Inner s) a -> (Outer a -> Expr s Bool) -> (Outer a -> Query s result) -> Query s result
  LeftJoin :: Query (Inner s) a -> (Outer a -> Expr s Bool) -> (Left a -> Query s result) -> Query s result
  Aggregate :: Query (Inner s) a -> (Aggregate a -> Query s result) -> Query s result
  GroupBy :: Expr (Inner s) a -> (AggregateExpr (Inner s) a -> Query (Inner s) result) -> Query (Inner s) result

data Order = Ascending | Descending
  deriving (Eq, Show)

instance Functor (Query s) where
  fmap f (Pure a) = Pure $ f a
  fmap f (Select table then_) = Select table $ fmap f <$> then_
  fmap f (Where cond then_) = Where cond $ f <$> then_
  fmap f (Order expr order_ then_) = Order expr order_ $ f <$> then_
  fmap f (Distinct q then_) = Distinct q $ fmap f <$> then_
  fmap f (InnerJoin q on then_) = InnerJoin q on $ fmap f <$> then_
  fmap f (LeftJoin q on then_) = LeftJoin q on $ fmap f <$> then_
  fmap f (Aggregate q then_) = Aggregate q $ fmap f <$> then_
  fmap f (GroupBy expr then_) = GroupBy expr $ fmap f <$> then_

instance Applicative (Query s) where
  pure = Pure
  (<*>) = ap

instance Monad (Query s) where
  Pure x >>= f = f x
  Select table then_ >>= f = Select table $ \row -> then_ row >>= f
  Where cond then_ >>= f = Where cond $ then_ >>= f
  Order expr order_ then_ >>= f = Order expr order_ $ then_ >>= f
  Distinct q then_ >>= f = Distinct q $ \row -> then_ row >>= f
  InnerJoin q on then_ >>= f = InnerJoin q on $ \row -> then_ row >>= f
  LeftJoin q on then_ >>= f = LeftJoin q on $ \row -> then_ row >>= f
  Aggregate q then_ >>= f = Aggregate q $ \agg -> then_ agg >>= f
  GroupBy expr then_ >>= f = GroupBy expr $ \agg -> then_ agg >>= f

type Row s table = HKD table (Expr s)

type NullableRow s table = HKD table (NullableExpr s)

type family Outer s where
  Outer (Expr (Inner s) a) = Expr s a
  Outer (Row (Inner s) table) = Row s table
  Outer (a, b) = (Outer a, Outer b)

type family Left s where
  Left (Expr (Inner s) a) = Expr s (Nullable a)
  Left (Row (Inner s) table) = NullableRow s table
  Left (a, b) = (Left a, Left b)

type family Aggregate s where
  Aggregate (AggregateExpr (Inner s) a) = Expr s a
  Aggregate (a, b) = (Aggregate a, Aggregate b)
