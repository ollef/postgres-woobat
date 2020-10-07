{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
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
import Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NonEmpty
import qualified Data.Text.Encoding as Text
import qualified Database.PostgreSQL.LibPQ as LibPQ
import Database.Woobat.Barbie hiding (result)
import qualified Database.Woobat.Barbie
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
  forall a m.
  ( Monad.MonadWoobat m
  , Barbie (Expr ()) a
  , HKD.AllB DatabaseType (ToBarbie (Expr ()) a)
  , HKD.ConstraintsB (ToBarbie (Expr ()) a)
  , Resultable (FromBarbie (Expr ()) a Identity)
  ) =>
  Select () a ->
  m [Result (FromBarbie (Expr ()) a Identity)]
select s =
  Monad.withConnection $ \connection -> liftIO $ do
    let (rawSQL, resultsBarbie) = compile s
        (code, params) = Raw.separateCodeAndParams rawSQL
        params' = fmap (\p -> (LibPQ.Oid 0, p, LibPQ.Binary)) <$> params
    maybeResult <- LibPQ.execParams connection code params' LibPQ.Binary
    case maybeResult of
      Nothing -> throwM $ Monad.ConnectionError LibPQ.ConnectionBad
      Just result -> do
        status <- LibPQ.resultStatus result
        let onError = do
              message <- LibPQ.resultErrorMessage result
              throwM $ Monad.ExecutionError status message
            onResult = do
              rowCount <- LibPQ.ntuples result
              forM [0 .. rowCount - 1] $ \rowNumber -> do
                let go :: DatabaseType x => Expr () x -> StateT LibPQ.Column IO (Identity x)
                    go _ = fmap Identity $ do
                      col <- get
                      put $ col + 1
                      maybeValue <- liftIO $ LibPQ.getvalue result rowNumber col
                      case (decoder, maybeValue) of
                        (Decoder d, Just v) ->
                          case Decoding.valueParser d v of
                            Left err ->
                              throwM $ Monad.DecodingError rowNumber col err
                            Right a ->
                              pure a
                        (Decoder _, Nothing) ->
                          throwM $ Monad.UnexpectedNullError rowNumber col
                        (NullableDecoder _, Nothing) ->
                          pure Nothing
                        (NullableDecoder d, Just v) ->
                          case Decoding.valueParser d v of
                            Left err ->
                              throwM $ Monad.DecodingError rowNumber col err
                            Right a ->
                              pure $ Just a

                barbieRow :: ToBarbie (Expr ()) a Identity <-
                  flip evalStateT 0 $ Barbie.btraverseC @DatabaseType go resultsBarbie
                pure $ Database.Woobat.Barbie.result $ fromBarbie @(Expr ()) @a barbieRow
        case status of
          LibPQ.EmptyQuery -> onError
          LibPQ.CommandOk -> onError
          LibPQ.CopyOut -> onError
          LibPQ.CopyIn -> onError
          LibPQ.CopyBoth -> onError
          LibPQ.BadResponse -> onError
          LibPQ.NonfatalError -> onError
          LibPQ.FatalError -> onError
          LibPQ.SingleTuple -> onResult
          LibPQ.TuplesOk -> onResult

compile :: forall a. Barbie (Expr ()) a => Select () a -> (Raw.SQL, ToBarbie (Expr ()) a (Expr ()))
compile s = do
  let (results, st) = run mempty s
      resultsBarbie :: ToBarbie (Expr ()) a (Expr ())
      resultsBarbie = toBarbie results
      sql = Compiler.compileSelect (Barbie.bfoldMap (\(Expr e) -> [e]) resultsBarbie) $ rawSelect st
  (sql, resultsBarbie)

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
  (Outer s a -> Expr t Bool) ->
  Select u (Left s a)
leftJoin (Select sel) on = Select $ do
  (innerResults, rightSelect) <- subquery sel
  let innerResultsBarbie :: ToBarbie (Expr (Inner s)) a (Expr (Inner s))
      innerResultsBarbie = toBarbie innerResults
  leftFrom <- gets $ Raw.from . rawSelect
  leftFrom' <- mapM (\() -> freshName "unit") leftFrom
  case rightSelect of
    Raw.Select rightFrom Raw.Empty Raw.Empty Raw.Empty -> do
      let Expr rawOn =
            on $ outer @s @a innerResultsBarbie
      rightFrom' <- mapM (\() -> freshName "unit") rightFrom
      modify $ \s ->
        s
          { rawSelect =
              (rawSelect s)
                { Raw.from =
                    Raw.LeftJoin leftFrom' rawOn rightFrom'
                }
          }
      return $ left @s @a innerResultsBarbie
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
            on $ outer @s @a outerResults
      modify $ \s ->
        s
          { rawSelect =
              (rawSelect s)
                { Raw.from =
                    Raw.LeftJoin
                      leftFrom'
                      rawOn
                      ( Raw.Subquery
                          (Barbie.bfoldMap (\(Pair (Const name) (Expr e)) -> pure (e, name)) namedResults)
                          rightSelect
                          alias
                      )
                }
          }
      return $ left @s @a outerResults

aggregate ::
  forall a s t.
  (Barbie (AggregateExpr (Inner s)) a, Same s t) =>
  Select (Inner s) a ->
  Select t (Aggregated s a)
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
  return $ aggregated @s @a outerResults

groupBy ::
  (Same s t, Same t u) =>
  Expr (Inner s) a ->
  Select (Inner t) (AggregateExpr (Inner u) a)
groupBy (Expr expr) = Select $ do
  addSelect mempty {Raw.groupBys = pure expr}
  pure $ AggregateExpr expr

-- unnest :: forall s t u a. (Same s t, Same t u, Unnestable a) => Expr s [a] -> Select t (Unnested u a)
-- unnest (Expr arr) = Select $ do
--   (returnRow, result) <- unnested @a
--   addSelect mempty {Raw.from = Raw.Set ("UNNEST(" <> arr <> ")") returnRow}
--   pure result

-- class Unnestable a where
--   type Unnested s a
--   type Unnested s a = Expr s a
--   unnested :: State SelectState (Raw.SQL, Unnested s a)
--   default unnested :: (Unnested s a ~ Expr s a) => State SelectState (Raw.SQL, Unnested s a)
--   unnested = do
--     alias <- Raw.code <$> freshName "unnested"
--     pure (alias, Expr alias)

-- instance {-# OVERLAPPING #-} HKD.AllB UnnestableRowElement (HKD table) => Unnestable table where
--   type Unnested s table = HKD table (Expr s)
--   unnested _ = do
--     rowAlias <- freshName "unnested"
--     undefined

-- class Unnestable a => UnnestableRowElement a

values :: forall s t u a. (Same s t, Same t u, Barbie (Expr (Inner s)) a) => NonEmpty a -> Select t (Outer s a)
values rows = Select $ do
  let firstBarbieRow :: ToBarbie (Expr (Inner s)) a (Expr (Inner s))
      barbieRows :: NonEmpty (ToBarbie (Expr (Inner s)) a (Expr (Inner s)))
      barbieRows@(firstBarbieRow NonEmpty.:| _) = toBarbie <$> rows
  rowAlias <- Raw.code <$> freshName "values"
  aliasesBarbie <- Barbie.btraverse (\_ -> Const <$> freshName "col") firstBarbieRow
  let aliases = Barbie.bfoldMap (\(Const a) -> [Raw.code a]) aliasesBarbie
  addSelect
    mempty
      { Raw.from =
          Raw.Set
            ( "(VALUES "
                <> Raw.separateBy ", " ((\row_ -> "(" <> Raw.separateBy ", " (Barbie.bfoldMap (\(Expr e) -> [e]) row_) <> ")") <$> barbieRows)
                <> ")"
            )
            ( rowAlias
                <> "("
                <> Raw.separateBy ", " aliases
                <> ")"
            )
      }
  let resultBarbie :: ToBarbie (Expr (Inner s)) a (Expr (Inner s))
      resultBarbie = Barbie.bmap (\(Const alias) -> Expr $ Raw.code alias) aliasesBarbie
  pure $ outer @s @a resultBarbie
