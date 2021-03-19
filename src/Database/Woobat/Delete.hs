{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.Woobat.Delete where

import qualified Barbies
import Data.Functor.Const
import qualified Data.HashMap.Lazy as HashMap
import qualified Data.Text.Encoding as Text
import qualified Database.PostgreSQL.LibPQ as LibPQ
import Database.Woobat.Barbie
import Database.Woobat.Delete.Builder (Delete)
import qualified Database.Woobat.Delete.Builder as Builder
import Database.Woobat.Expr
import Database.Woobat.Monad (MonadWoobat)
import qualified Database.Woobat.Raw as Raw
import Database.Woobat.Returning
import qualified Database.Woobat.Select as Select
import Database.Woobat.Table (Table)
import qualified Database.Woobat.Table as Table

delete ::
  forall table a m.
  (MonadWoobat m, Barbies.TraversableB table) =>
  Table table ->
  (table Expr -> Delete (Returning a)) ->
  m a
delete table query =
  Raw.execute statement getResults
  where
    columnNames = Barbies.bmap (\(Const name) -> Const $ Text.encodeUtf8 name) $ Table.columnNames table
    columnNameExprs = Barbies.bmap (\(Const name) -> Expr $ Raw.codeExpr name) columnNames
    columnNamesList = Barbies.bfoldMap (\(Const name) -> [name]) columnNames
    usedNames = HashMap.fromList $ (Text.encodeUtf8 $ Table.name table, 1) : [(name, 1) | name <- columnNamesList]
    (returning, builderState) = Builder.run usedNames $ query columnNameExprs
    tableName = Text.encodeUtf8 $ Table.name table
    returningClause :: Raw.SQL
    getResults :: LibPQ.Result -> IO a
    (returningClause, getResults) = case returning of
      ReturningNothing ->
        ("", const $ pure ())
      Returning results -> do
        let resultsBarbie = toBarbie results
            resultsExprs = Barbies.bfoldMap (\(Expr e) -> [Raw.unExpr e usedNames]) resultsBarbie
        (" RETURNING " <> Raw.separateBy ", " resultsExprs, Select.parseRows (Just results) resultsBarbie)
      ReturningRowCount ->
        ("", fmap (\(LibPQ.Row r) -> fromIntegral r) <$> LibPQ.ntuples)
    using =
      case Raw.unitView $ Builder.rawFrom builderState of
        Right () -> ""
        Left f -> " USING " <> Raw.compileFrom f
    statement =
      "DELETE FROM " <> Raw.code tableName
        <> using
        <> Raw.compileWheres (Builder.wheres builderState)
        <> returningClause

delete_ ::
  forall table m.
  (MonadWoobat m, Barbies.TraversableB table) =>
  Table table ->
  (table Expr -> Delete ()) ->
  m ()
delete_ table query = delete table $ fmap returningNothing <$> query
