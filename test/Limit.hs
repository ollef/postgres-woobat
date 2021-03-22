{-# LANGUAGE OverloadedStrings #-}

module Limit where

import Data.Generics.Labels ()
import qualified Database.Woobat.Raw as Raw
import qualified Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

properties :: [(Hedgehog.PropertyName, Hedgehog.Property)]
properties =
  [
    ( "left identity"
    , Hedgehog.property $ do
        limit <- Hedgehog.forAll genLimit
        mempty <> limit Hedgehog.=== limit
    )
  ,
    ( "right identity"
    , Hedgehog.property $ do
        limit <- Hedgehog.forAll genLimit
        limit <> mempty Hedgehog.=== limit
    )
  ,
    ( "associativity"
    , Hedgehog.property $ do
        limit1 <- Hedgehog.forAll genLimit
        limit2 <- Hedgehog.forAll genLimit
        limit3 <- Hedgehog.forAll genLimit
        limit1 <> (limit2 <> limit3) Hedgehog.=== (limit1 <> limit2) <> limit3
    )
  ,
    ( "apply mempty == id"
    , Hedgehog.property $ do
        xs <- Hedgehog.forAll $ Gen.list (Range.linearFrom 0 0 1000) $ Gen.int Range.linearBounded
        apply mempty xs Hedgehog.=== xs
    )
  ,
    ( "apply l1 . apply l2 == apply (l1 <> l2)"
    , Hedgehog.property $ do
        l1 <- Hedgehog.forAll genLimit
        l2 <- Hedgehog.forAll genLimit
        xs <- Hedgehog.forAll $ Gen.list (Range.linearFrom 0 0 1000) $ Gen.int Range.linearBounded
        apply l1 (apply l2 xs) Hedgehog.=== apply (l1 <> l2) xs
    )
  ]

genLimit :: Hedgehog.Gen Raw.Limit
genLimit = do
  count <- Gen.maybe $ Gen.int $ Range.linearFrom 0 0 100
  offset <- Gen.int $ Range.linearFrom 0 0 100
  pure $ Raw.Limit {Raw.count = count, Raw.offset = offset}

apply :: Raw.Limit -> [a] -> [a]
apply limit =
  maybe id take (Raw.count limit) . drop (Raw.offset limit)
