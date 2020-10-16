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

module Database.Woobat.Query where

import Control.Monad.State
import qualified Data.Barbie as Barbie
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as Lazy
import Data.Functor.Const (Const (Const))
import Data.Generic.HKD (HKD)
import qualified Data.Generic.HKD as HKD
import Data.Int
import Data.Scientific
import Data.Text (Text)
import qualified Data.Text.Encoding as Text
import qualified Data.Text.Lazy as Lazy
import Data.Time (Day, DiffTime, LocalTime, TimeOfDay, TimeZone, UTCTime)
import Data.UUID.Types (UUID)
import Database.Woobat.Barbie hiding (result)
import Database.Woobat.Expr
import Database.Woobat.Query.Monad
import qualified Database.Woobat.Raw as Raw
import Database.Woobat.Scope
import Database.Woobat.Select.Builder
import Database.Woobat.Table (Table)
import qualified Database.Woobat.Table as Table

from ::
  forall table s query.
  (MonadQuery query, HKD.FunctorB (HKD table)) =>
  Table table ->
  query s (HKD table (Expr s))
from table = do
  let tableName =
        Text.encodeUtf8 $ Table.name table
  alias <- freshName tableName
  let tableRow :: HKD table (Expr s)
      tableRow =
        HKD.bmap (\(Const columnName) -> Expr $ Raw.codeExpr $ alias <> "." <> Text.encodeUtf8 columnName) $ Table.columnNames table
  addFrom $ Raw.Table tableName alias
  pure tableRow

where_ :: (MonadQuery query, Same s t) => Expr s Bool -> query t ()
where_ (Expr cond) =
  addWhere cond

filter_ :: (Same s t, Same t u) => (a -> Expr s Bool) -> Select t a -> Select u a
filter_ f q = do
  a <- q
  where_ $ f a
  pure a

leftJoin ::
  forall a s t u query.
  (MonadQuery query, Barbie (Expr (Inner s)) a) =>
  Select (Inner s) a ->
  (Outer s a -> Expr t Bool) ->
  query u (Left s a)
leftJoin (Select sel) on = do
  (innerResults, rightSelect) <- subquery sel
  let innerResultsBarbie :: ToBarbie (Expr (Inner s)) a (Expr (Inner s))
      innerResultsBarbie = toBarbie innerResults
  leftFrom <- getFrom
  leftFrom' <- mapM (\() -> freshName "unit") leftFrom
  usedNames_ <- getUsedNames
  alias <- freshName "subquery"
  namedResults :: ToBarbie (Expr (Inner s)) a (Product (Const ByteString) (Expr (Inner s))) <-
    HKD.btraverse
      ( \e -> do
          name <- freshName "col"
          pure $ Product (Const name) e
      )
      innerResultsBarbie
  let outerResults :: ToBarbie (Expr (Inner s)) a (Expr (Inner s))
      outerResults =
        HKD.bmap (\(Product (Const name) _) -> Expr $ Raw.codeExpr $ alias <> "." <> name) namedResults
      Expr rawOn =
        on $ outer @s @a outerResults
  putFrom $
    Raw.LeftJoin
      leftFrom'
      (Raw.unExpr rawOn usedNames_)
      ( Raw.Subquery
          (Barbie.bfoldMap (\(Product (Const name) (Expr e)) -> pure (Raw.unExpr e usedNames_, name)) namedResults)
          rightSelect
          alias
      )
  return $ left @s @a outerResults

aggregate ::
  forall a s t query.
  (MonadQuery query, Barbie (AggregateExpr (Inner s)) a, Same s t) =>
  Select (Inner s) a ->
  query t (Aggregated s a)
aggregate (Select sel) = do
  (innerResults, aggSelect) <- subquery sel
  alias <- freshName "subquery"
  usedNames_ <- getUsedNames
  namedResults :: ToBarbie (AggregateExpr (Inner s)) a (Product (Const ByteString) (AggregateExpr (Inner s))) <-
    HKD.btraverse
      ( \e -> do
          name <- freshName "col"
          pure $ Product (Const name) e
      )
      (toBarbie innerResults)
  let outerResults :: ToBarbie (AggregateExpr (Inner s)) a (Expr (Inner s))
      outerResults =
        HKD.bmap (\(Product (Const name) _) -> Expr $ Raw.codeExpr $ alias <> "." <> name) namedResults
  addFrom $
    Raw.Subquery
      (Barbie.bfoldMap (\(Product (Const name) (AggregateExpr e)) -> pure (Raw.unExpr e usedNames_, name)) namedResults)
      aggSelect
      alias
  return $ aggregated @s @a outerResults

groupBy ::
  (Same s t, Same t u) =>
  Expr (Inner s) a ->
  Select (Inner t) (AggregateExpr (Inner u) a)
groupBy (Expr expr) = Select $ do
  usedNames_ <- gets usedNames
  addSelect mempty {Raw.groupBys = pure $ Raw.unExpr expr usedNames_}
  pure $ AggregateExpr expr

-- | @VALUES@
values :: Same s t => DatabaseType a => [a] -> Select t (Expr s a)
values = expressions . map value

-- | @VALUES@
expressions ::
  forall s t u a query.
  ( MonadQuery query
  , Same s t
  , Same t u
  , Barbie (Expr (Inner s)) a
  , Monoid (ToBarbie (Expr (Inner u)) a (Const ()))
  , HKD.ConstraintsB (ToBarbie (Expr (Inner u)) a)
  , HKD.AllB DatabaseType (ToBarbie (Expr (Inner u)) a)
  ) =>
  [a] ->
  query t (Outer s a)
expressions rows = do
  case rows of
    [] -> where_ false
    _ -> pure ()
  let barbieRows :: [ToBarbie (Expr (Inner s)) a (Expr (Inner s))]
      barbieRows = toBarbie <$> rows
  rowAlias <- Raw.code <$> freshName "expressions"
  aliasesBarbie :: ToBarbie (Expr (Inner s)) a (Const ByteString) <- Barbie.btraverse (\(Const ()) -> Const <$> freshName "col") mempty
  let aliases = Barbie.bfoldMap (\(Const a) -> [Raw.code a]) aliasesBarbie
  usedNames_ <- getUsedNames
  addFrom $
    Raw.Set
      ( "(VALUES "
          <> ( case barbieRows of
                [] -> do
                  let go :: forall f x. DatabaseType x => f x -> Expr (Inner s) x
                      go _ = Expr $ "null::" <> typeName @x
                      nullRow = Barbie.bmapC @DatabaseType go aliasesBarbie
                  "(" <> Raw.separateBy ", " (Barbie.bfoldMap (\(Expr e) -> [Raw.unExpr e usedNames_]) nullRow) <> ")"
                _ -> Raw.separateBy ", " ((\row_ -> "(" <> Raw.separateBy ", " (Barbie.bfoldMap (\(Expr e) -> [Raw.unExpr e usedNames_]) row_) <> ")") <$> barbieRows)
             )
          <> ")"
      )
      ( rowAlias
          <> "("
          <> Raw.separateBy ", " aliases
          <> ")"
      )
  let resultBarbie :: ToBarbie (Expr (Inner s)) a (Expr (Inner s))
      resultBarbie = Barbie.bmap (\(Const alias) -> Expr $ Raw.codeExpr alias) aliasesBarbie
  pure $ outer @s @a resultBarbie

-- | @UNNEST@
unnest ::
  forall s t a query.
  ( MonadQuery query
  , Same s t
  , Unnestable a
  ) =>
  Expr s [a] ->
  query t (Unnested a (Expr s))
unnest (Expr arr) = do
  (returnRow, result) <- unnested @a @s
  usedNames_ <- getUsedNames
  addFrom $ Raw.Set ("UNNEST(" <> Raw.unExpr arr usedNames_ <> ")") returnRow
  pure result

-- TODO move
type Unnested a f = FromBarbie f (UnnestedBarbie a f) f

class Unnestable a where
  type UnnestedBarbie a :: (* -> *) -> *
  type UnnestedBarbie a = Singleton a
  unnested :: forall s query. MonadQuery query => query s (Raw.SQL, Unnested a (Expr s))
  default unnested :: (MonadQuery query, UnnestedBarbie a ~ Singleton a) => query s (Raw.SQL, Unnested a (Expr s))
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
    usedNames_ <- getUsedNames
    let returnRowList = Barbie.bfoldMap (\(Const (colAlias, typeName_)) -> [colAlias <> " " <> Raw.unExpr typeName_ usedNames_]) returnRow
        result = Barbie.bmap (\(Const (colAlias, _)) -> Expr $ Raw.Expr $ const colAlias) returnRow
    alias <- Raw.code <$> freshName "unnested"
    pure (alias <> "(" <> Raw.separateBy ", " returnRowList <> ")", result)
    where
      go :: forall a s query. (UnnestableRowElement a, MonadQuery query) => Const () a -> query s (Const (Raw.SQL, Raw.Expr) a)
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
