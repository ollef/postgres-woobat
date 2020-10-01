{-# language AllowAmbiguousTypes #-}
{-# language FlexibleContexts #-}
{-# language OverloadedStrings #-}
{-# language ScopedTypeVariables #-}
{-# language TypeApplications #-}
{-# language TypeFamilies #-}
{-# options_ghc -Wno-redundant-constraints #-}
module Database.Woobat.Select
  ( module Database.Woobat.Select
  , Database.Woobat.Barbie.Barbie
  , Database.Woobat.Select.Builder.Select
  )
  where

import Control.Monad.State
import qualified Data.Barbie as Barbie
import Data.ByteString (ByteString)
import Data.Functor.Const (Const(Const))
import Data.Functor.Product
import qualified Data.Generic.HKD as HKD
import Data.Generic.HKD (HKD)
import qualified Data.Text.Encoding as Text
import Database.Woobat.Barbie
import qualified Database.Woobat.Compiler as Compiler
import Database.Woobat.Expr
import qualified Database.Woobat.Raw as Raw
import Database.Woobat.Scope
import Database.Woobat.Select.Builder
import Database.Woobat.Table (Table)
import qualified Database.Woobat.Table as Table

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
  -> Select s (HKD table (Expr s))
from table = Select $ do
  let
    tableName =
      Text.encodeUtf8 $ Table.name table
  alias <- freshNameWithSuggestion tableName
  let
    tableRow :: HKD table (Expr s)
    tableRow =
      HKD.bmap (\(Const columnName) -> Expr $ Raw.code $ alias <> "." <> Text.encodeUtf8 columnName) $ Table.columnNames table
  addSelect mempty { Raw.from = Raw.Table tableName alias }
  pure tableRow

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

ascending :: Raw.Order
ascending = Raw.Ascending

descending :: Raw.Order
descending = Raw.Descending

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
