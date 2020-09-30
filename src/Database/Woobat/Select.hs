{-# language AllowAmbiguousTypes #-}
{-# language FlexibleContexts #-}
{-# language FlexibleInstances #-}
{-# language MultiParamTypeClasses #-}
{-# language OverloadedStrings #-}
{-# language ScopedTypeVariables #-}
{-# language TypeApplications #-}
{-# language TypeFamilies #-}
{-# options_ghc -Wno-redundant-constraints #-}
module Database.Woobat.Select
  ( module Database.Woobat.Select
  , Database.Woobat.Select.Builder.Select
  )
  where

import Control.Monad.State
import qualified Data.Barbie as Barbie
import Data.ByteString (ByteString)
import Data.Coerce
import Data.Functor.Const (Const(Const))
import Data.Functor.Product
import qualified Data.Generic.HKD as HKD
import Data.Generic.HKD (HKD)
import qualified Data.Text.Encoding as Text
import qualified Database.Woobat.Compiler as Compiler
import Database.Woobat.Expr
import qualified Database.Woobat.Raw as Raw
import Database.Woobat.Scope
import Database.Woobat.Select.Builder
import Database.Woobat.Table (Table)
import qualified Database.Woobat.Table as Table

newtype SingletonBarbie a f = SingletonBarbie (f a)

instance HKD.FunctorB (SingletonBarbie a) where
  bmap f (SingletonBarbie x) = SingletonBarbie $ f x

instance HKD.TraversableB (SingletonBarbie a) where
  btraverse f (SingletonBarbie x) = SingletonBarbie <$> f x

instance Barbie f () where
  type ToBarbie f () = Barbie.Unit
  type FromBarbie f () g = ()
  toBarbie () = Barbie.Unit
  unBarbie Barbie.Unit = ()
  fromBarbie Barbie.Unit = ()

class HKD.TraversableB (ToBarbie f t) => Barbie (f :: * -> *) t where
  type ToBarbie f t :: (* -> *) -> *
  type FromBarbie f t (g :: * -> *)
  toBarbie :: t -> ToBarbie f t f
  unBarbie :: ToBarbie f t f -> t -- TODO: Remove if unused
  fromBarbie :: ToBarbie f t g -> FromBarbie f t g

instance Same s t => Barbie (Expr s) (Expr t a) where
  type ToBarbie (Expr s) (Expr t a) = SingletonBarbie a
  type FromBarbie (Expr s) (Expr t a) g = g a
  toBarbie = SingletonBarbie
  unBarbie (SingletonBarbie x) = x
  fromBarbie (SingletonBarbie x) = x

instance Same s t => Barbie (Expr s) (NullableExpr t a) where
  type ToBarbie (Expr s) (NullableExpr t a) = SingletonBarbie (Nullable a)
  type FromBarbie (Expr s) (NullableExpr t a) g = g (Nullable a)
  toBarbie (NullableExpr e) = SingletonBarbie e
  unBarbie (SingletonBarbie x) = NullableExpr x
  fromBarbie (SingletonBarbie x) = x

instance Same s t => Barbie (AggregateExpr s) (AggregateExpr t a) where
  type ToBarbie (AggregateExpr s) (AggregateExpr t a) = SingletonBarbie a
  type FromBarbie (AggregateExpr s) (AggregateExpr t a) g = g a
  toBarbie = SingletonBarbie
  unBarbie (SingletonBarbie x) = x
  fromBarbie (SingletonBarbie x) = x

instance HKD.TraversableB (HKD table) => Barbie f (HKD table f) where
  type ToBarbie f (HKD table f) = HKD table
  type FromBarbie f (HKD table f) g = HKD table g
  toBarbie = id
  unBarbie = id
  fromBarbie = id

instance (Barbie f a, Barbie f b) => Barbie f (a, b) where
  type ToBarbie f (a, b) = Product (ToBarbie f a) (ToBarbie f b)
  type FromBarbie f (a, b) g = (FromBarbie f a g, FromBarbie f b g)
  toBarbie (a, b) = Pair (toBarbie a) (toBarbie b)
  unBarbie (Pair a b) = (unBarbie a, unBarbie b)
  fromBarbie (Pair a b) = (fromBarbie @f @a a, fromBarbie @f @b b)

type Row s table = HKD table (Expr s)

type ToOuter s a = FromBarbie (Expr (Inner s)) a (Expr s)

toOuter
  :: forall s a. (Barbie (Expr (Inner s)) a)
  => ToBarbie (Expr (Inner s)) a (Expr (Inner s))
  -> ToOuter s a
toOuter =
  fromBarbie @(Expr (Inner s)) @a
  . HKD.bmap (coerce :: forall x. Expr (Inner s) x -> Expr s x)

type ToLeft s a = FromBarbie (Expr (Inner s)) a (NullableExpr s)

toLeft
  :: forall s a. (Barbie (Expr (Inner s)) a)
  => ToBarbie (Expr (Inner s)) a (Expr (Inner s))
  -> ToLeft s a
toLeft =
  fromBarbie @(Expr (Inner s)) @a
  . HKD.bmap (coerce :: forall x. Expr (Inner s) x -> NullableExpr s x)

type FromAggregate s a = FromBarbie (AggregateExpr (Inner s)) a (Expr s)

fromAggregate
  :: forall s a. (Barbie (AggregateExpr (Inner s)) a)
  => ToBarbie (AggregateExpr (Inner s)) a (Expr (Inner s))
  -> FromAggregate s a
fromAggregate =
  fromBarbie @(AggregateExpr (Inner s)) @a
  . HKD.bmap (coerce :: forall x. Expr (Inner s) x -> Expr s x)

-------------------------------------------------------------------------------

select :: forall s a. Barbie (Expr s) a => Select s a -> Raw.SQL
select (Select s) =
  evalState compiler $ usedNames selectState
  where
    (results, selectState) =
      runState s SelectState {usedNames = mempty, rawSelect = mempty}
    resultsBarbie :: ToBarbie (Expr s) a (Expr s)
    resultsBarbie = toBarbie results
    Compiler.Compiler compiler =
      Compiler.compile resultsBarbie $ rawSelect selectState

from
  :: forall table s
  . HKD.FunctorB (HKD table)
  => Table table
  -> Select s (Row s table)
from table = Select $ do
  let
    tableName =
      Text.encodeUtf8 $ Table.name table
  alias <- freshNameWithSuggestion tableName
  let
    row :: Row s table
    row =
      HKD.bmap (\(Const columnName) -> Expr $ Raw.code $ alias <> "." <> Text.encodeUtf8 columnName) $ Table.columnNames table
  addSelect mempty { Raw.from = Raw.Table tableName alias }
  pure row

where_ :: Same s t => Expr s Bool -> Select t ()
where_ (Expr cond) =
  Select $ addSelect mempty { Raw.wheres = pure cond }

filter_ :: (Same s t, Same t u) => (a -> Expr s Bool) -> Select t a -> Select u a
filter_ f q = do
  a <- q
  where_ $ f a
  pure a

orderBy :: Same s t => Expr s a -> Raw.Order -> Select t ()
orderBy (Expr expr) order_ =
  Select $ addSelect mempty { Raw.orderBys = pure (expr, order_) }

leftJoin
  :: forall a s t u. (Barbie (Expr (Inner s)) a)
  => Select (Inner s) a
  -> (ToOuter s a -> Expr t Bool)
  -> Select u (ToLeft s a)
leftJoin (Select sel) on = Select $ do
  (rightSelect, innerResults) <- subquery sel
  let
    innerResultsBarbie :: ToBarbie (Expr (Inner s)) a (Expr (Inner s))
    innerResultsBarbie = toBarbie innerResults
  case rightSelect of
    Raw.Select rightFrom Raw.Empty Raw.Empty Raw.Empty -> do
      let
        Expr rawOn =
          on $ toOuter @s @a innerResultsBarbie
      modify $ \s -> s
        { rawSelect = (rawSelect s)
          { Raw.from =
            Raw.LeftJoin (Raw.from $ rawSelect s) rawOn rightFrom
          }
        }
      return $ toLeft @s @a innerResultsBarbie

    _ -> do
      alias <- freshNameWithSuggestion "subquery"
      namedResults :: ToBarbie (Expr (Inner s)) a (Product (Const ByteString) (Expr (Inner s)))
        <- HKD.btraverse
          (\e -> do
            name <- freshNameWithSuggestion "col"
            pure $ Pair (Const name) e
          )
          innerResultsBarbie
      let
        outerResults :: ToBarbie (Expr (Inner s)) a (Expr (Inner s))
        outerResults =
          HKD.bmap (\(Pair (Const name) _) -> Expr $ Raw.code $ alias <> "." <> name) namedResults
        Expr rawOn =
          on $ toOuter @s @a outerResults
      modify $ \s -> s
        { rawSelect = (rawSelect s)
          { Raw.from =
            Raw.LeftJoin
              (Raw.from $ rawSelect s)
              rawOn
              (Raw.Subquery
                (Barbie.bfoldMap (\(Pair (Const name) (Expr e)) -> pure (e, name)) namedResults)
                rightSelect
                alias
              )
          }
        }
      return $ toLeft @s @a outerResults

aggregate
  :: forall a s t. (Barbie (AggregateExpr (Inner s)) a, Same s t)
  => Select (Inner s) a
  -> Select t (FromAggregate s a)
aggregate (Select sel) = Select $ do
  (aggSelect, innerResults) <- subquery sel
  alias <- freshNameWithSuggestion "subquery"
  namedResults :: ToBarbie (AggregateExpr (Inner s)) a (Product (Const ByteString) (AggregateExpr (Inner s)))
    <- HKD.btraverse
      (\e -> do
        name <- freshNameWithSuggestion "col"
        pure $ Pair (Const name) e
      )
      (toBarbie innerResults)
  let
    outerResults :: ToBarbie (AggregateExpr (Inner s)) a (Expr (Inner s))
    outerResults =
      HKD.bmap (\(Pair (Const name) _) -> Expr $ Raw.code $ alias <> "." <> name) namedResults
  addSelect mempty
    { Raw.from =
      Raw.Subquery
        (Barbie.bfoldMap (\(Pair (Const name) (AggregateExpr e)) -> pure (e, name)) namedResults)
        aggSelect
        alias
    }
  return $ fromAggregate @s @a outerResults

groupBy
  :: (Same s t, Same t u)
  => Expr (Inner s) a
  -> Select (Inner t) (AggregateExpr (Inner u) a)
groupBy (Expr expr) = Select $ do
  addSelect mempty { Raw.groupBys = pure expr }
  pure $ AggregateExpr expr
