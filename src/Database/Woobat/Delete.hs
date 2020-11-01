{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.Woobat.Delete where

import qualified Data.Barbie as Barbie
import Data.Functor.Const
import Data.Generic.HKD (HKD)
import qualified Data.Generic.HKD as HKD
import qualified Data.HashMap.Lazy as HashMap
import qualified Data.Text.Encoding as Text
import qualified Database.PostgreSQL.LibPQ as LibPQ
import Database.Woobat.Barbie
import qualified Database.Woobat.Compiler as Compiler
import Database.Woobat.Delete.Builder (Delete)
import qualified Database.Woobat.Delete.Builder as Builder
import Database.Woobat.Expr
import Database.Woobat.Monad (MonadWoobat)
import qualified Database.Woobat.Raw as Raw
import Database.Woobat.Returning (Returning (..))
import qualified Database.Woobat.Select as Select
import Database.Woobat.Table (Table)
import qualified Database.Woobat.Table as Table

delete ::
  forall table a m.
  (MonadWoobat m, HKD.TraversableB (HKD table)) =>
  Table table ->
  (HKD table (Expr ()) -> Delete () (Returning a)) ->
  m a
delete table query =
  Raw.execute statement getResults
  where
    columnNames = HKD.bmap (\(Const name) -> Const $ Text.encodeUtf8 name) $ Table.columnNames table
    columnNameExprs = HKD.bmap (\(Const name) -> Expr $ Raw.codeExpr name) columnNames
    columnNamesList = Barbie.bfoldMap (\(Const name) -> [name]) columnNames
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
            resultsExprs = Barbie.bfoldMap (\(Expr e) -> [Raw.unExpr e usedNames]) resultsBarbie
        (" RETURNING " <> Raw.separateBy ", " resultsExprs, Select.parseRows (Just results) resultsBarbie)
      ReturningRowCount ->
        ("", fmap (\(LibPQ.Row r) -> fromIntegral r) <$> LibPQ.ntuples)
    using =
      case Raw.unitView $ Builder.rawFrom builderState of
        Right () -> ""
        Left f -> " USING " <> Compiler.compileFrom f
    statement =
      "DELETE FROM " <> Raw.code tableName
        <> using
        <> Compiler.compileWheres (Builder.wheres builderState)
        <> returningClause
