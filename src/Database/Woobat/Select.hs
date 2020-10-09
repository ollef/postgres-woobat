{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module Database.Woobat.Select (
  module Database.Woobat.Select,
  Database.Woobat.Barbie.Barbie,
  Database.Woobat.Select.Builder.Select,
) where

import Control.Exception.Safe
import Control.Monad
import Control.Monad.State
import qualified Data.Barbie as Barbie
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as Lazy
import Data.Functor.Const (Const (Const))
import Data.Functor.Identity
import Data.Functor.Product
import Data.Generic.HKD (HKD)
import qualified Data.Generic.HKD as HKD
import Data.Int
import Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NonEmpty
import Data.Scientific
import Data.Text (Text)
import qualified Data.Text.Encoding as Text
import qualified Data.Text.Lazy as Lazy
import Data.Time (Day, DiffTime, LocalTime, TimeOfDay, TimeZone, UTCTime)
import Data.UUID.Types (UUID)
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
      sql = Compiler.compileSelect (Barbie.bfoldMap (\(Expr e) -> [Raw.unExpr e $ usedNames st]) resultsBarbie) $ rawSelect st
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
        HKD.bmap (\(Const columnName) -> Expr $ Raw.codeExpr $ alias <> "." <> Text.encodeUtf8 columnName) $ Table.columnNames table
  addSelect mempty {Raw.from = Raw.Table tableName alias}
  pure tableRow

where_ :: Same s t => Expr s Bool -> Select t ()
where_ (Expr cond) =
  Select $ do
    usedNames_ <- gets usedNames
    addSelect mempty {Raw.wheres = pure $ Raw.unExpr cond usedNames_}

filter_ :: (Same s t, Same t u) => (a -> Expr s Bool) -> Select t a -> Select u a
filter_ f q = do
  a <- q
  where_ $ f a
  pure a

orderBy :: Same s t => Expr s a -> Raw.Order -> Select t ()
orderBy (Expr expr) order_ =
  Select $ do
    usedNames_ <- gets usedNames
    addSelect mempty {Raw.orderBys = pure (Raw.unExpr expr usedNames_, order_)}

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
  usedNames_ <- gets usedNames
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
                    Raw.LeftJoin leftFrom' (Raw.unExpr rawOn usedNames_) rightFrom'
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
            HKD.bmap (\(Pair (Const name) _) -> Expr $ Raw.codeExpr $ alias <> "." <> name) namedResults
          Expr rawOn =
            on $ outer @s @a outerResults
      modify $ \s ->
        s
          { rawSelect =
              (rawSelect s)
                { Raw.from =
                    Raw.LeftJoin
                      leftFrom'
                      (Raw.unExpr rawOn usedNames_)
                      ( Raw.Subquery
                          (Barbie.bfoldMap (\(Pair (Const name) (Expr e)) -> pure (Raw.unExpr e usedNames_, name)) namedResults)
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
  usedNames_ <- gets usedNames
  namedResults :: ToBarbie (AggregateExpr (Inner s)) a (Product (Const ByteString) (AggregateExpr (Inner s))) <-
    HKD.btraverse
      ( \e -> do
          name <- freshName "col"
          pure $ Pair (Const name) e
      )
      (toBarbie innerResults)
  let outerResults :: ToBarbie (AggregateExpr (Inner s)) a (Expr (Inner s))
      outerResults =
        HKD.bmap (\(Pair (Const name) _) -> Expr $ Raw.codeExpr $ alias <> "." <> name) namedResults
  addSelect
    mempty
      { Raw.from =
          Raw.Subquery
            (Barbie.bfoldMap (\(Pair (Const name) (AggregateExpr e)) -> pure (Raw.unExpr e usedNames_, name)) namedResults)
            aggSelect
            alias
      }
  return $ aggregated @s @a outerResults

groupBy ::
  (Same s t, Same t u) =>
  Expr (Inner s) a ->
  Select (Inner t) (AggregateExpr (Inner u) a)
groupBy (Expr expr) = Select $ do
  usedNames_ <- gets usedNames
  addSelect mempty {Raw.groupBys = pure $ Raw.unExpr expr usedNames_}
  pure $ AggregateExpr expr

values :: forall s t u a. (Same s t, Same t u, Barbie (Expr (Inner s)) a) => NonEmpty a -> Select t (Outer s a)
values rows = Select $ do
  let firstBarbieRow :: ToBarbie (Expr (Inner s)) a (Expr (Inner s))
      barbieRows :: NonEmpty (ToBarbie (Expr (Inner s)) a (Expr (Inner s)))
      barbieRows@(firstBarbieRow NonEmpty.:| _) = toBarbie <$> rows
  rowAlias <- Raw.code <$> freshName "values"
  aliasesBarbie <- Barbie.btraverse (\_ -> Const <$> freshName "col") firstBarbieRow
  let aliases = Barbie.bfoldMap (\(Const a) -> [Raw.code a]) aliasesBarbie
  usedNames_ <- gets usedNames
  addSelect
    mempty
      { Raw.from =
          Raw.Set
            ( "(VALUES "
                <> Raw.separateBy ", " ((\row_ -> "(" <> Raw.separateBy ", " (Barbie.bfoldMap (\(Expr e) -> [Raw.unExpr e usedNames_]) row_) <> ")") <$> barbieRows)
                <> ")"
            )
            ( rowAlias
                <> "("
                <> Raw.separateBy ", " aliases
                <> ")"
            )
      }
  let resultBarbie :: ToBarbie (Expr (Inner s)) a (Expr (Inner s))
      resultBarbie = Barbie.bmap (\(Const alias) -> Expr $ Raw.codeExpr alias) aliasesBarbie
  pure $ outer @s @a resultBarbie

-- | @UNNEST@
unnest ::
  forall s t a.
  ( Same s t
  , Unnestable a
  ) =>
  Expr s [a] ->
  Select t (Unnested a (Expr s))
unnest (Expr arr) = Select $ do
  (returnRow, result) <- unnested @a @s
  usedNames_ <- gets usedNames
  addSelect mempty {Raw.from = Raw.Set ("UNNEST(" <> Raw.unExpr arr usedNames_ <> ")") returnRow}
  pure result

-- TODO move
type Unnested a f = FromBarbie f (UnnestedBarbie a f) f

class Unnestable a where
  type UnnestedBarbie a :: (* -> *) -> *
  type UnnestedBarbie a = Singleton a
  unnested :: State SelectState (Raw.SQL, Unnested a (Expr s))
  default unnested :: (UnnestedBarbie a ~ Singleton a) => State SelectState (Raw.SQL, Unnested a (Expr s))
  unnested = do
    alias <- Raw.code <$> freshName "unnested"
    pure (alias, Expr $ Raw.Expr $ const alias)

instance Unnestable [a]
instance Unnestable (JSONB a)
instance UnnestableRowElement a => Unnestable (Maybe a)
instance Unnestable Bool
instance Unnestable Int
instance Unnestable Int16
instance Unnestable Int32
instance Unnestable Int64
instance Unnestable Float
instance Unnestable Double
instance Unnestable Scientific
instance Unnestable UUID
instance Unnestable Char
instance Unnestable Text
instance Unnestable Lazy.Text
instance Unnestable ByteString
instance Unnestable Lazy.ByteString
instance Unnestable Day
instance Unnestable TimeOfDay
instance Unnestable (TimeOfDay, TimeZone)
instance Unnestable LocalTime
instance Unnestable UTCTime
instance Unnestable DiffTime

instance
  ( HKD.AllB UnnestableRowElement row
  , HKD.ConstraintsB row
  , HKD.TraversableB row
  , Monoid (row (Const ()))
  ) =>
  Unnestable (Row row)
  where
  type UnnestedBarbie (Row row) = RowF row
  unnested = do
    returnRow <- Barbie.btraverseC @UnnestableRowElement go (mempty :: row (Const ()))
    usedNames_ <- gets usedNames
    let returnRowList = Barbie.bfoldMap (\(Const (colAlias, typeName_)) -> [colAlias <> " " <> Raw.unExpr typeName_ usedNames_]) returnRow
        result = Barbie.bmap (\(Const (colAlias, _)) -> Expr $ Raw.Expr $ const colAlias) returnRow
    pure ("(" <> Raw.separateBy ", " returnRowList <> ")", result)
    where
      go :: forall a. UnnestableRowElement a => Const () a -> State SelectState (Const (Raw.SQL, Raw.Expr) a)
      go (Const ()) = do
        colAlias <- freshName "col"
        pure $ Const (Raw.code colAlias, typeName @a)

class (DatabaseType a, Unnestable a, UnnestedBarbie a ~ Singleton a) => UnnestableRowElement a
instance (DatabaseType a, Unnestable a, UnnestedBarbie a ~ Singleton a) => UnnestableRowElement a

-- | Unnest a singleton array
unrow ::
  ( HKD.AllB UnnestableRowElement row
  , HKD.AllB DatabaseType row
  , HKD.ConstraintsB row
  , HKD.TraversableB row
  , Monoid (row (Const ()))
  , Same s t
  ) =>
  Expr s (Row row) ->
  Select t (row (Expr s))
unrow row_ = unnest $ array [row_]
