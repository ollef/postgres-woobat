{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module Database.Woobat.Select (
  module Database.Woobat.Select,
  Database.Woobat.Barbie.Barbie,
  Database.Woobat.Select.Builder.Select,
) where

import Control.Exception.Safe
import Control.Monad.State
import qualified Data.Barbie as Barbie
import Data.ByteString (ByteString)
import Data.Functor.Const (Const (Const))
import Data.Functor.Identity
import Data.Functor.Product
import Data.Generic.HKD (HKD)
import qualified Data.Generic.HKD as HKD
import qualified Data.Text.Encoding as Text
import qualified Database.PostgreSQL.LibPQ as LibPQ
import Database.Woobat.Barbie
import qualified Database.Woobat.Compiler as Compiler
import Database.Woobat.Expr
import qualified Database.Woobat.Monad as Monad
import qualified Database.Woobat.Raw as Raw
import Database.Woobat.Scope
import Database.Woobat.Select.Builder
import Database.Woobat.Table (Table)
import qualified Database.Woobat.Table as Table
import qualified PostgreSQL.Binary.Decoding as Decoding

select ::
  forall s a m.
  ( Monad.MonadWoobat m
  , Barbie (Expr s) a
  , HKD.AllB DatabaseType (ToBarbie (Expr s) a)
  , HKD.ConstraintsB (ToBarbie (Expr s) a)
  , Result (FromBarbie (Expr s) a Identity)
  ) =>
  Select s a ->
  m [ToResult (FromBarbie (Expr s) a Identity)]
select s = do
  let (rawSQL, resultsBarbie) = compile s
      (code, params) = Raw.separateCodeAndParams rawSQL
  Monad.withConnection $ \connection -> liftIO $ do
    maybeResult <-
      LibPQ.execParams
        connection
        code
        (fmap (\p -> (LibPQ.Oid 0, p, LibPQ.Binary)) <$> params)
        LibPQ.Binary
    case maybeResult of
      Nothing -> throwM $ Monad.ConnectionError LibPQ.ConnectionBad
      Just result -> do
        status <- LibPQ.resultStatus result
        let onError = do
              message <- LibPQ.resultErrorMessage result
              throwM $ Monad.ExecutionError status message
        case status of
          LibPQ.BadResponse -> onError
          LibPQ.NonfatalError -> onError
          LibPQ.FatalError -> onError
          _ -> do
            rowCount <- LibPQ.ntuples result
            forM [0 .. rowCount - 1] $ \rowNumber -> do
              let go :: DatabaseType x => Expr s x -> StateT LibPQ.Column IO (Identity x)
                  go _ = fmap Identity $ do
                    col <- get
                    put $ col + 1
                    maybeValue <- liftIO $ LibPQ.getvalue result rowNumber col
                    case (decoder, maybeValue) of
                      (Decoder d, Just value) ->
                        case Decoding.valueParser d value of
                          Left err ->
                            throwM $ Monad.DecodingError rowNumber col err
                          Right a ->
                            pure a
                      (Decoder _, Nothing) ->
                        throwM $ Monad.UnexpectedNullError rowNumber col
                      (NullableDecoder _, Nothing) ->
                        pure Nothing
                      (NullableDecoder d, Just value) ->
                        case Decoding.valueParser d value of
                          Left err ->
                            throwM $ Monad.DecodingError rowNumber col err
                          Right a ->
                            pure $ Just a

              barbieRow :: ToBarbie (Expr s) a Identity <-
                flip evalStateT 0 $ Barbie.btraverseC @DatabaseType go resultsBarbie
              pure $ toResult $ fromBarbie @(Expr s) @a barbieRow

compile :: forall s a. Barbie (Expr s) a => Select s a -> (Raw.SQL, ToBarbie (Expr s) a (Expr s))
compile s = do
  let (results, usedNames', rawSelect') = run mempty s
      resultsBarbie :: ToBarbie (Expr s) a (Expr s)
      resultsBarbie = toBarbie results
      compiler = Compiler.compileSelect (Barbie.bfoldMap (\(Expr e) -> [e]) resultsBarbie) rawSelect'
  (fst $ Compiler.run usedNames' compiler, resultsBarbie)

from ::
  forall table s.
  HKD.FunctorB (HKD table) =>
  Table table ->
  Select s (HKD table (Expr s))
from table = Select $ do
  let tableName =
        Text.encodeUtf8 $ Table.name table
  alias <- freshName tableName
  let tableRow :: HKD table (Expr s)
      tableRow =
        HKD.bmap (\(Const columnName) -> Expr $ Raw.code $ alias <> "." <> Text.encodeUtf8 columnName) $ Table.columnNames table
  addSelect mempty {Raw.from = Raw.Table tableName alias}
  pure tableRow

where_ :: Same s t => Expr s Bool -> Select t ()
where_ (Expr cond) =
  Select $ addSelect mempty {Raw.wheres = pure cond}

filter_ :: (Same s t, Same t u) => (a -> Expr s Bool) -> Select t a -> Select u a
filter_ f q = do
  a <- q
  where_ $ f a
  pure a

orderBy :: Same s t => Expr s a -> Raw.Order -> Select t ()
orderBy (Expr expr) order_ =
  Select $ addSelect mempty {Raw.orderBys = pure (expr, order_)}

ascending :: Raw.Order
ascending = Raw.Ascending

descending :: Raw.Order
descending = Raw.Descending

leftJoin ::
  forall a s t u.
  (Barbie (Expr (Inner s)) a) =>
  Select (Inner s) a ->
  (ToOuter s a -> Expr t Bool) ->
  Select u (ToLeft s a)
leftJoin (Select sel) on = Select $ do
  (innerResults, rightSelect) <- subquery sel
  let innerResultsBarbie :: ToBarbie (Expr (Inner s)) a (Expr (Inner s))
      innerResultsBarbie = toBarbie innerResults
  case rightSelect of
    Raw.Select rightFrom Raw.Empty Raw.Empty Raw.Empty -> do
      let Expr rawOn =
            on $ toOuter @s @a innerResultsBarbie
      modify $ \s ->
        s
          { rawSelect =
              (rawSelect s)
                { Raw.from =
                    Raw.LeftJoin (Raw.from $ rawSelect s) rawOn rightFrom
                }
          }
      return $ toLeft @s @a innerResultsBarbie
    _ -> do
      alias <- freshName "subquery"
      namedResults :: ToBarbie (Expr (Inner s)) a (Product (Const ByteString) (Expr (Inner s))) <-
        HKD.btraverse
          ( \e -> do
              name <- freshName "col"
              pure $ Pair (Const name) e
          )
          innerResultsBarbie
      let outerResults :: ToBarbie (Expr (Inner s)) a (Expr (Inner s))
          outerResults =
            HKD.bmap (\(Pair (Const name) _) -> Expr $ Raw.code $ alias <> "." <> name) namedResults
          Expr rawOn =
            on $ toOuter @s @a outerResults
      modify $ \s ->
        s
          { rawSelect =
              (rawSelect s)
                { Raw.from =
                    Raw.LeftJoin
                      (Raw.from $ rawSelect s)
                      rawOn
                      ( Raw.Subquery
                          (Barbie.bfoldMap (\(Pair (Const name) (Expr e)) -> pure (e, name)) namedResults)
                          rightSelect
                          alias
                      )
                }
          }
      return $ toLeft @s @a outerResults

aggregate ::
  forall a s t.
  (Barbie (AggregateExpr (Inner s)) a, Same s t) =>
  Select (Inner s) a ->
  Select t (FromAggregate s a)
aggregate (Select sel) = Select $ do
  (innerResults, aggSelect) <- subquery sel
  alias <- freshName "subquery"
  namedResults :: ToBarbie (AggregateExpr (Inner s)) a (Product (Const ByteString) (AggregateExpr (Inner s))) <-
    HKD.btraverse
      ( \e -> do
          name <- freshName "col"
          pure $ Pair (Const name) e
      )
      (toBarbie innerResults)
  let outerResults :: ToBarbie (AggregateExpr (Inner s)) a (Expr (Inner s))
      outerResults =
        HKD.bmap (\(Pair (Const name) _) -> Expr $ Raw.code $ alias <> "." <> name) namedResults
  addSelect
    mempty
      { Raw.from =
          Raw.Subquery
            (Barbie.bfoldMap (\(Pair (Const name) (AggregateExpr e)) -> pure (e, name)) namedResults)
            aggSelect
            alias
      }
  return $ fromAggregate @s @a outerResults

groupBy ::
  (Same s t, Same t u) =>
  Expr (Inner s) a ->
  Select (Inner t) (AggregateExpr (Inner u) a)
groupBy (Expr expr) = Select $ do
  addSelect mempty {Raw.groupBys = pure expr}
  pure $ AggregateExpr expr

unnest :: (Same s t, Same t u) => Expr s [a] -> Select t (Expr u a)
unnest (Expr arr) = Select $ do
  alias <- freshName "unnested_array"
  addSelect mempty {Raw.from = Raw.Set ("UNNEST(" <> arr <> ")") alias}
  pure $ Expr $ Raw.code alias
