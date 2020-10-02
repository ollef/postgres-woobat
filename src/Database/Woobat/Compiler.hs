{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.Woobat.Compiler where

import Control.Monad.State
import Data.ByteString (ByteString)
import Data.Foldable
import Data.HashMap.Lazy (HashMap)
import qualified Data.HashMap.Lazy as HashMap
import Data.List
import Data.String
import qualified Database.Woobat.Raw as Raw

newtype Compiler a = Compiler (State (HashMap ByteString Int) a)
  deriving (Functor, Applicative, Monad, MonadState (HashMap ByteString Int))

run :: HashMap ByteString Int -> Compiler a -> (a, HashMap ByteString Int)
run usedNames (Compiler s) = runState s usedNames

instance Monoid a => Monoid (Compiler a) where
  mempty = pure mempty

instance Semigroup a => Semigroup (Compiler a) where
  ma <> mb = (<>) <$> ma <*> mb

instance IsString a => IsString (Compiler a) where
  fromString = pure . fromString

freshName :: ByteString -> Compiler ByteString
freshName suggestion = Compiler $ do
  used <- get
  let usedCount = HashMap.lookupDefault 0 suggestion used
  put $ HashMap.insert suggestion (usedCount + 1) used
  pure $
    if usedCount == 0
      then suggestion
      else suggestion <> "_" <> fromString (show usedCount)

compile :: [Raw.SQL] -> Raw.Select -> Compiler Raw.SQL
compile result select =
  compileSelect (separateBy ", " result) select

compileSelect :: Raw.SQL -> Raw.Select -> Compiler Raw.SQL
compileSelect exprs Raw.Select {from = Raw.Unit, wheres = Raw.Empty, groupBys = Raw.Empty, orderBys = Raw.Empty} =
  "SELECT " <> pure exprs
compileSelect exprs Raw.Select {from, wheres, groupBys, orderBys} =
  "SELECT " <> pure exprs <> " FROM "
    <> compileFrom from
    <> pure (compileWheres wheres)
    <> pure (compileGroupBys groupBys)
    <> pure (compileOrderBys orderBys)

compileFrom :: Raw.From -> Compiler Raw.SQL
compileFrom from =
  case from of
    Raw.Unit ->
      "(VALUES (0)) " <> fmap Raw.code (freshName "unit")
    Raw.Table name alias
      | name == alias ->
        pure $ Raw.code name
      | otherwise ->
        pure $ Raw.code name <> " AS " <> Raw.code alias
    Raw.Set expr alias ->
      pure $ expr <> " AS " <> Raw.code alias
    Raw.Subquery exprAliases select alias ->
      "(" <> compileSelect (separateBy ", " $ (\(expr, columnAlias) -> expr <> " AS " <> Raw.code columnAlias) <$> exprAliases) select <> ") AS " <> pure (Raw.code alias)
    Raw.CrossJoin from_ froms ->
      separateBy ", " $ compileFrom <$> from_ : toList froms
    Raw.LeftJoin left on right ->
      compileFrom left <> " LEFT JOIN " <> compileFrom right <> " ON " <> pure on

compileWheres :: Raw.Tsil Raw.SQL -> Raw.SQL
compileWheres Raw.Empty = mempty
compileWheres wheres = " WHERE " <> separateBy " AND " wheres

compileGroupBys :: Raw.Tsil Raw.SQL -> Raw.SQL
compileGroupBys Raw.Empty = mempty
compileGroupBys groupBys = " GROUP BY " <> separateBy ", " groupBys

compileOrderBys :: Raw.Tsil (Raw.SQL, Raw.Order) -> Raw.SQL
compileOrderBys Raw.Empty = mempty
compileOrderBys orderBys =
  " ORDER BY "
    <> separateBy ", " ((\(expr, order) -> expr <> " " <> compileOrder order) <$> orderBys)

compileOrder :: Raw.Order -> Raw.SQL
compileOrder Raw.Ascending = "ASC"
compileOrder Raw.Descending = "DESC"

separateBy :: (Foldable f, Monoid a) => a -> f a -> a
separateBy separator =
  mconcat . intersperse separator . toList
