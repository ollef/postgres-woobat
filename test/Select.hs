{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Select where

import Data.Foldable
import qualified Data.List.NonEmpty as NonEmpty
import Database.Woobat
import qualified Expr
import qualified Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import Woobat (runWoobat)

properties :: [(Hedgehog.PropertyName, Hedgehog.Property)]
properties =
  [
    ( "values"
    , Hedgehog.property $ do
        Expr.Some gen <- Hedgehog.forAll Expr.genSome
        xs <- Hedgehog.forAll $ Gen.nonEmpty (Range.linearFrom 0 0 10) gen
        result <-
          Hedgehog.evalM $
            runWoobat $ select $ values $ value <$> xs
        result Hedgehog.=== toList xs
    )
  ,
    ( "multiple values"
    , Hedgehog.property $ do
        Expr.Some gen <- Hedgehog.forAll Expr.genSome
        xs <- Hedgehog.forAll $ Gen.nonEmpty (Range.linearFrom 0 0 10) gen
        result <-
          Hedgehog.evalM $
            runWoobat $
              select $
                values $ NonEmpty.zip (value <$> xs) (value <$> xs)
        result Hedgehog.=== zip (toList xs) (toList xs)
    )
    -- ( "start with leftJoin"
    -- , Hedgehog.property $ do
    --     Expr.Some gen <- Hedgehog.forAll Expr.genSome
    --     xs <- Hedgehog.forAll $ Gen.nonEmpty (Range.linearFrom 0 0 10) gen
    --     let sameParamAs :: f a -> g a -> g a
    --         sameParamAs _ ga = ga
    --     result <-
    --       Hedgehog.evalM $
    --         runWoobat $ select $ leftJoin (values $ value <$> xs) $ const false
    --     result Hedgehog.=== [sameParamAs xs Nothing]
    -- )
  ]
