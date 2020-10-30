{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.Woobat.Compiler where

import Data.ByteString (ByteString)
import qualified Database.Woobat.Raw as Raw

compileSelect :: [Raw.SQL] -> Raw.Select -> Raw.SQL
compileSelect exprs Raw.Select {from, wheres, groupBys, orderBys} =
  "SELECT " <> Raw.separateBy ", " exprs
    <> ( case Raw.unitView from of
          Right () -> mempty
          Left from' -> " FROM " <> compileFrom from'
       )
    <> compileWheres wheres
    <> compileGroupBys groupBys
    <> compileOrderBys orderBys

compileFrom :: Raw.From ByteString -> Raw.SQL
compileFrom from =
  case from of
    Raw.Unit alias ->
      "(VALUES (0)) " <> Raw.code alias
    Raw.Table name alias
      | name == alias ->
        Raw.code name
      | otherwise ->
        Raw.code name <> " AS " <> Raw.code alias
    Raw.Set expr returnRow ->
      expr <> " AS " <> returnRow
    Raw.Subquery exprAliases select alias ->
      "(" <> compileSelect [expr <> " AS " <> Raw.code columnAlias | (expr, columnAlias) <- exprAliases] select <> ") AS " <> Raw.code alias
    Raw.CrossJoin left right ->
      compileFrom left <> " CROSS JOIN " <> compileFrom right
    Raw.LeftJoin left on right ->
      compileFrom left <> " LEFT JOIN " <> compileFrom right <> " ON " <> on

compileWheres :: Raw.Tsil Raw.SQL -> Raw.SQL
compileWheres Raw.Empty = mempty
compileWheres wheres = " WHERE " <> Raw.separateBy " AND " wheres

compileGroupBys :: Raw.Tsil Raw.SQL -> Raw.SQL
compileGroupBys Raw.Empty = mempty
compileGroupBys groupBys = " GROUP BY " <> Raw.separateBy ", " groupBys

compileOrderBys :: Raw.Tsil (Raw.SQL, Raw.Order) -> Raw.SQL
compileOrderBys Raw.Empty = mempty
compileOrderBys orderBys =
  " ORDER BY "
    <> Raw.separateBy ", " ((\(expr, order) -> expr <> " " <> compileOrder order) <$> orderBys)

compileOrder :: Raw.Order -> Raw.SQL
compileOrder Raw.Ascending = "ASC"
compileOrder Raw.Descending = "DESC"

compileOnConflict :: Raw.OnConflict -> Raw.Expr
compileOnConflict onConflict =
  Raw.Expr $ \usedNames -> case onConflict of
    Raw.NoConflictHandling -> ""
    Raw.OnAnyConflictDoNothing -> " ON CONFLICT DO NOTHING"
    Raw.OnConflict fields assignments maybeWhere -> do
      " ON CONFLICT (" <> Raw.separateBy ", " (Raw.code <$> fields) <> ") "
        <> case assignments of
          [] -> "DO NOTHING"
          _ -> "DO UPDATE " <> Raw.separateBy ", " ["SET " <> Raw.code f <> " = " <> Raw.unExpr e usedNames | (f, e) <- assignments]
        <> case maybeWhere of
          Nothing -> ""
          Just e -> " WHERE " <> Raw.unExpr e usedNames
