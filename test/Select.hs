{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Select where

import Database.Woobat
import qualified Expr
import GHC.Generics
import qualified Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import Woobat (runWoobat)

properties :: [(Hedgehog.PropertyName, Hedgehog.Property)]
properties =
  [
    ( "start with leftJoin"
    , Hedgehog.property $ do
        Expr.SomeNonNested gen <- Hedgehog.forAll Expr.genSomeNonNested
        xs <- Hedgehog.forAll $ Gen.list (Range.linearFrom 0 0 10) gen
        let sameParamAs :: f a -> g a -> g a
            sameParamAs _ ga = ga
        result <-
          Hedgehog.evalM $
            runWoobat $ select $ leftJoin (unnest $ value xs) $ const false
        result Hedgehog.=== [sameParamAs xs Nothing]
    )
  ]
