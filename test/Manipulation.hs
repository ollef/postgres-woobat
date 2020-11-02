{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Manipulation where

import qualified Data.Barbie as Barbie
import Data.Functor.Const
import Data.Generic.HKD (HKD)
import qualified Data.Generic.HKD as HKD
import Data.Text (Text)
import qualified Data.Text.Encoding as Text
import Database.Woobat
import qualified Database.Woobat.Raw as Raw
import qualified Database.Woobat.Table as Table

create ::
  ( MonadWoobat m
  , HKD.TraversableB (HKD table)
  , HKD.AllB DatabaseType (HKD table)
  , HKD.ConstraintsB (HKD table)
  ) =>
  Table table ->
  m ()
create table_ =
  Raw.execute statement $ const $ pure ()
  where
    columns = Barbie.bmapC @DatabaseType go $ Table.columnNames table_
    columnsList = Barbie.bfoldMap (\(Const col) -> [col]) columns
    go :: forall a. DatabaseType a => Const Text a -> Const Raw.SQL a
    go (Const name) =
      Const $
        Raw.code (Text.encodeUtf8 name) <> " " <> Raw.unExpr (typeName @a) mempty
          <> case decoder @a of
            Decoder _ -> " NOT NULL"
            NullableDecoder _ -> ""
    tableName = Text.encodeUtf8 $ Table.name table_
    statement = "CREATE TABLE " <> Raw.code tableName <> " (" <> Raw.separateBy ", " columnsList <> ");"
