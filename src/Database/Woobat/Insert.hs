{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.Woobat.Insert where

import Control.Lens (Lens', (^.))
import qualified Data.Barbie as Barbie
import Data.ByteString (ByteString)
import Data.Functor.Const
import Data.Generic.HKD (HKD)
import qualified Data.Generic.HKD as HKD
import qualified Data.HashMap.Lazy as HashMap
import qualified Data.Text.Encoding as Text
import qualified Database.PostgreSQL.LibPQ as LibPQ
import Database.Woobat.Barbie
import qualified Database.Woobat.Compiler as Compiler
import Database.Woobat.Expr
import Database.Woobat.Monad (MonadWoobat)
import Database.Woobat.Query.Monad
import qualified Database.Woobat.Raw as Raw
import Database.Woobat.Returning (Returning (..))
import Database.Woobat.Select (Select)
import qualified Database.Woobat.Select as Select
import Database.Woobat.Table (Table)
import qualified Database.Woobat.Table as Table

insert ::
  forall table a m.
  (MonadWoobat m, HKD.TraversableB (HKD table)) =>
  Table table ->
  Select () (HKD table (Expr ())) ->
  OnConflict table ->
  (HKD table (Expr ()) -> Returning a) ->
  m a
insert table query (OnConflict onConflict_) returning =
  Raw.execute statement getResults
  where
    columnNames = HKD.bmap (\(Const name) -> Const $ Text.encodeUtf8 name) $ Table.columnNames table
    columnNameExprs = HKD.bmap (\(Const name) -> Expr $ Raw.codeExpr name) columnNames
    excluded_ = HKD.bmap (\(Const name) -> Expr $ "EXCLUDED." <> Raw.codeExpr name) columnNames
    columnNamesList = Barbie.bfoldMap (\(Const name) -> [name]) columnNames
    usedNames = HashMap.fromList $ (Text.encodeUtf8 $ Table.name table, 1) : [(name, 1) | name <- columnNamesList]
    (compiledQuery, _) = Select.compile $ do
      putUsedNames usedNames
      query
    tableName = Text.encodeUtf8 $ Table.name table
    onConflictClause =
      Raw.unExpr
        (Compiler.compileOnConflict $ onConflict_ table ConflictContext {existing = columnNameExprs, excluded = excluded_})
        usedNames
    returningClause :: Raw.SQL
    getResults :: LibPQ.Result -> IO a
    (returningClause, getResults) = case returning columnNameExprs of
      ReturningNothing ->
        ("", const $ pure ())
      Returning results -> do
        let resultsBarbie = toBarbie results
            resultsExprs = Barbie.bfoldMap (\(Expr e) -> [Raw.unExpr e usedNames]) resultsBarbie
        (" RETURNING " <> Raw.separateBy ", " resultsExprs, Select.parseRows (Just results) resultsBarbie)
      ReturningRowCount ->
        ("", fmap (\(LibPQ.Row r) -> fromIntegral r) <$> LibPQ.ntuples)
    statement =
      "INSERT INTO " <> Raw.code tableName <> " (" <> Raw.separateBy ", " (Raw.code <$> columnNamesList) <> ")"
        <> " ("
        <> compiledQuery
        <> ")"
        <> onConflictClause
        <> returningClause

newtype OnConflict table = OnConflict (Table table -> ConflictContext table -> Raw.OnConflict)

data ConflictContext table = ConflictContext
  { -- | The existing data in the table row
    existing :: HKD table (Expr ())
  , -- | The data that caused a conflict during insertion, i.e. the new data
    excluded :: HKD table (Expr ())
  }

noConflictHandling :: OnConflict table
noConflictHandling =
  OnConflict $ \_ _ -> Raw.NoConflictHandling

onAnyConflictDoNothing :: OnConflict table
onAnyConflictDoNothing =
  OnConflict $ \_ _ -> Raw.OnAnyConflictDoNothing

onConflict :: [ConflictingField table] -> (ConflictContext table -> ConflictAction table) -> OnConflict table
onConflict fields action =
  OnConflict $ \table conflictContext -> do
    let ConflictAction action' = action conflictContext
        (assignments, maybeWhere) = action' table
    Raw.OnConflict
      [Text.encodeUtf8 $ getConst $ Table.columnNames table ^. f | ConflictingField f <- fields]
      assignments
      maybeWhere

data ConflictingField table where
  ConflictingField :: (forall f. Lens' (HKD table f) (f a)) -> ConflictingField table

newtype ConflictAction table = ConflictAction (Table table -> ([(ByteString, Raw.Expr)], Maybe Raw.Expr))

data Assignment table where
  (:=) :: (forall f. Lens' (HKD table f) (f a)) -> Expr () a -> Assignment table

setAll ::
  (Barbie.TraversableB (HKD table), Barbie.ProductB (HKD table)) =>
  HKD table (Expr ()) ->
  ConflictAction table
setAll assignments = ConflictAction $ \table ->
  ( Barbie.bfoldMap (\(Const x) -> [x]) $
      Barbie.bzipWith
        (\(Const f) (Expr e) -> Const (Text.encodeUtf8 f, e))
        (Table.columnNames table)
        assignments
  , Nothing
  )

set :: [Assignment table] -> ConflictAction table
set assignments = ConflictAction $ \table ->
  ( [(Text.encodeUtf8 $ getConst $ Table.columnNames table ^. f, e) | f := Expr e <- assignments]
  , Nothing
  )

doNothing :: a -> ConflictAction table
doNothing = const $ set []

where_ :: ConflictAction table -> Expr () Bool -> ConflictAction table
where_ (ConflictAction f) (Expr where') = ConflictAction $ \table -> do
  let (assignments, maybeWhere) = f table
  case maybeWhere of
    Nothing ->
      (assignments, Just where')
    Just existingWhere -> do
      let Expr where'' = Expr existingWhere &&. Expr where'
      (assignments, Just where'')
