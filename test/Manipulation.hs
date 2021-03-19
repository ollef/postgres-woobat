{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Manipulation where

import Prelude hiding (drop)

import qualified Barbies
import Data.Functor.Const
import Data.Generic.HKD (HKD)
import qualified Data.Generic.HKD as HKD
import Data.Generics.Labels ()
import Data.List (sort)
import Data.Text (Text)
import qualified Data.Text.Encoding as Text
import Database.Woobat
import qualified Database.Woobat.Raw as Raw
import qualified Database.Woobat.Table as Table
import qualified Expr
import qualified Hedgehog
import qualified Select

create ::
  ( MonadWoobat m
  , Barbies.TraversableB table
  , Barbies.AllB DatabaseType table
  , Barbies.ConstraintsB table
  ) =>
  Table table ->
  m ()
create table_ =
  Raw.execute statement $ const $ pure ()
  where
    columns = Barbies.bmapC @DatabaseType go $ Table.columnNames table_
    columnsList = Barbies.bfoldMap (\(Const col) -> [col]) columns
    go :: forall a. DatabaseType a => Const Text a -> Const Raw.SQL a
    go (Const name) =
      Const $
        Raw.code (Text.encodeUtf8 name) <> " " <> Raw.unExpr (typeName @a) mempty
          <> case decoder @a of
            Decoder _ -> " NOT NULL"
            NullableDecoder _ -> ""
    tableName = Text.encodeUtf8 $ Table.name table_
    statement = "CREATE TABLE " <> Raw.code tableName <> " (" <> Raw.separateBy ", " columnsList <> ");"

drop :: MonadWoobat m => Table table -> m ()
drop table_ =
  Raw.execute statement $ const $ pure ()
  where
    tableName = Text.encodeUtf8 $ Table.name table_
    statement = "DROP TABLE IF EXISTS " <> Raw.code tableName

properties ::
  (forall a. WoobatT (Hedgehog.PropertyT IO) a -> Hedgehog.PropertyT IO a) ->
  [(Hedgehog.PropertyName, Hedgehog.Property)]
properties runWoobat =
  [
    ( "Select after insert"
    , Hedgehog.property $ do
        Select.SomeColumnSelectSpec select1 expected1 <- Hedgehog.forAll Select.genSomeColumnSelectSpec
        Select.SomeColumnSelectSpec select2 expected2 <- Hedgehog.forAll Select.genSomeColumnSelectSpec
        let select_ :: forall a b. Select (Expr a) -> Select (Expr b) -> Select (HKD (Expr.TableTwo a b) Expr)
            select_ s1 s2 = do
              x1 <- s1
              x2 <- s2
              pure $ HKD.build @(Expr.TableTwo _ _) x1 x2
            expected = Expr.TableTwo <$> expected1 <*> expected2
        let table_ = table "select_after_insert"
        result <- runWoobat $ do
          drop table_
          create table_
          insert table_ (select_ select1 select2) noConflictHandling returningNothing
          select $ from table_
        sort result Hedgehog.=== sort expected
    )
  ,
    ( "Insert returning"
    , Hedgehog.property $ do
        Select.SomeColumnSelectSpec select1 expected1 <- Hedgehog.forAll Select.genSomeColumnSelectSpec
        Select.SomeColumnSelectSpec select2 expected2 <- Hedgehog.forAll Select.genSomeColumnSelectSpec
        let select_ :: forall a b. Select (Expr a) -> Select (Expr b) -> Select (HKD (Expr.TableTwo a b) Expr)
            select_ s1 s2 = do
              x1 <- s1
              x2 <- s2
              pure $ HKD.build @(Expr.TableTwo _ _) x1 x2
            expected = Expr.TableTwo <$> expected1 <*> expected2
        let table_ = table "insert_returning"
        result <- runWoobat $ do
          drop table_
          create table_
          insert table_ (select_ select1 select2) noConflictHandling Returning
        sort result Hedgehog.=== sort expected
    )
  ,
    ( "Insert returning row count"
    , Hedgehog.property $ do
        Select.SomeColumnSelectSpec select1 expected1 <- Hedgehog.forAll Select.genSomeColumnSelectSpec
        Select.SomeColumnSelectSpec select2 expected2 <- Hedgehog.forAll Select.genSomeColumnSelectSpec
        let select_ :: forall a b. Select (Expr a) -> Select (Expr b) -> Select (HKD (Expr.TableTwo a b) Expr)
            select_ s1 s2 = do
              x1 <- s1
              x2 <- s2
              pure $ HKD.build @(Expr.TableTwo _ _) x1 x2
            expected = Expr.TableTwo <$> expected1 <*> expected2
        let table_ = table "insert_returning_row_count"
        result <- runWoobat $ do
          drop table_
          create table_
          insert table_ (select_ select1 select2) noConflictHandling returningRowCount
        result Hedgehog.=== length expected
    )
  ]
