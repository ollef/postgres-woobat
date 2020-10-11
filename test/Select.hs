{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Select where

import Control.Lens hiding ((<.))
import Control.Monad
import Data.Foldable
import qualified Data.Generic.HKD as HKD
import Data.Generics.Labels ()
import qualified Data.List as List
import Data.Ratio
import Database.Woobat
import qualified Database.Woobat.Barbie as Barbie
import qualified Expr
import qualified Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

properties ::
  (forall a. WoobatT (Hedgehog.PropertyT IO) a -> Hedgehog.PropertyT IO a) ->
  [(Hedgehog.PropertyName, Hedgehog.Property)]
properties runWoobat =
  [
    ( "values"
    , Hedgehog.property $ do
        Expr.Some gen <- Hedgehog.forAll Expr.genSome
        xs <- Hedgehog.forAll $ Gen.list (Range.linearFrom 0 0 10) gen
        result <-
          Hedgehog.evalM $
            runWoobat $ select $ values $ value <$> xs
        result Hedgehog.=== xs
    )
  ,
    ( "where_"
    , Hedgehog.property $ do
        Expr.SomeIntegral gen <- Hedgehog.forAll Expr.genSomeIntegral
        cutoff <- Hedgehog.forAll gen
        xs <- Hedgehog.forAll $ Gen.list (Range.linearFrom 0 0 10) gen
        result <-
          Hedgehog.evalM $
            runWoobat $
              select $ do
                v <- values $ value <$> xs
                where_ $ v >. value cutoff
                pure v
        result Hedgehog.=== filter (> cutoff) xs
    )
  ,
    ( "orderBy"
    , Hedgehog.property $ do
        Expr.SomeIntegral gen <- Hedgehog.forAll Expr.genSomeIntegral
        xs <- Hedgehog.forAll $ Gen.list (Range.linearFrom 0 0 10) gen
        result <-
          Hedgehog.evalM $
            runWoobat $
              select $ do
                (v, v') <- values $ zip (value <$> xs) (value <$> reverse xs)
                orderBy v ascending
                pure (v, v')
        result Hedgehog.=== List.sortOn fst (zip xs (reverse xs))
    )
  ,
    ( "leftJoin"
    , Hedgehog.property $ do
        Expr.SomeIntegral gen <- Hedgehog.forAll Expr.genSomeIntegral
        xs <- Hedgehog.forAll $ Gen.list (Range.linearFrom 0 0 10) gen
        Operation _ dbOp haskellOp <- Hedgehog.forAll genOrdOperation
        result <-
          Hedgehog.evalM $
            runWoobat $
              select $ do
                v <- values $ value <$> xs
                mv' <- leftJoin (values $ value <$> xs) $ dbOp v
                pure (v, mv')
        result Hedgehog.=== do
          v <- xs
          mv <- leftJoinList xs $ haskellOp v
          pure (v, mv)
    )
  ,
    ( "aggregate"
    , Hedgehog.property $ do
        Expr.SomeIntegral gen <- Hedgehog.forAll Expr.genSomeIntegral
        xs <- Hedgehog.forAll $ Gen.list (Range.linearFrom 0 0 10) gen
        result <-
          Hedgehog.evalM $
            runWoobat $
              select $
                aggregate $ do
                  v <- values $ value <$> xs
                  pure ((count v, countAll), (all_ $ v ==. 0, any_ $ v ==. 0), (max_ v, min_ v), sum_ v, arrayAggregate v)
        result
          Hedgehog.=== [
                         ( (length xs, length xs)
                         , (all (== 0) xs, any (== 0) xs)
                         , if null xs
                            then (Nothing, Nothing)
                            else (Just $ maximum xs, Just $ minimum xs)
                         , sum $ fromIntegral <$> xs
                         , xs
                         )
                       ]
    )
  ,
    ( "aggregate average"
    , Hedgehog.property $ do
        Expr.SomeIntegral gen <- Hedgehog.forAll $ Expr.genSomeRangedIntegral $ Range.linearFrom 0 (-1000) 1000
        xs <- Hedgehog.forAll (Gen.list (Range.linearFrom 0 0 10) gen)
        result <-
          Hedgehog.evalM $
            runWoobat $
              select $
                aggregate $ do
                  v <- values $ value <$> xs
                  pure $ average v
        let result' = fmap round <$> result
            expected :: Ratio Integer
            expected = sum (fromIntegral <$> xs) % fromIntegral (length xs)
        result' Hedgehog.=== [if null xs then Nothing else Just (round expected :: Integer)]
    )
  ,
    ( "multiple values"
    , Hedgehog.property $ do
        Expr.Some gen <- Hedgehog.forAll Expr.genSome
        xs <- Hedgehog.forAll $ Gen.list (Range.linearFrom 0 0 10) gen
        result <-
          Hedgehog.evalM $
            runWoobat $
              select $
                values $ zip (value <$> xs) (value <$> xs)
        result Hedgehog.=== zip (toList xs) (toList xs)
    )
  ,
    ( "start with leftJoin"
    , Hedgehog.property $ do
        Expr.SomeNonMaybe gen <- Hedgehog.forAll Expr.genSomeNonMaybe
        xs <- Hedgehog.forAll $ Gen.list (Range.linearFrom 0 0 10) gen
        let sameParamAs :: f a -> g a -> g a
            sameParamAs _ ga = ga
        result <-
          Hedgehog.evalM $
            runWoobat $ select $ leftJoin (values $ value <$> xs) $ const false
        result Hedgehog.=== [sameParamAs xs Nothing]
    )
  ,
    ( "unnest integral"
    , Hedgehog.property $ do
        Expr.SomeIntegral gen <- Hedgehog.forAll Expr.genSomeIntegral
        xs <- Hedgehog.forAll $ Gen.list (Range.linearFrom 0 0 10) gen
        result <-
          Hedgehog.evalM $
            runWoobat $
              select $
                unnest (value xs)
        result Hedgehog.=== xs
    )
  ,
    ( "unnest row"
    , Hedgehog.property $ do
        Expr.SomeIntegral gen1 <- Hedgehog.forAll Expr.genSomeIntegral
        Expr.SomeIntegral gen2 <- Hedgehog.forAll Expr.genSomeIntegral
        xs <- Hedgehog.forAll $ Gen.list (Range.linearFrom 0 0 10) $ Expr.TableTwo <$> gen1 <*> gen2
        result <-
          Hedgehog.evalM $
            runWoobat $
              select $ do
                tab <- unnest (array $ row . record <$> xs)
                where_ $ tab ^. #field1 ==. tab ^. #field1 &&. tab ^. #field2 ==. tab ^. #field2
                pure tab
        result Hedgehog.=== xs
    )
  ,
    ( "exists"
    , Hedgehog.property $ do
        Expr.SomeIntegral gen <- Hedgehog.forAll Expr.genSomeIntegral
        xs <- Hedgehog.forAll $ Gen.list (Range.linearFrom 0 0 10) gen
        result <-
          Hedgehog.evalM $
            runWoobat $
              select $
                pure $ exists $ unnest (value xs)
        result Hedgehog.=== [not (List.null xs)]
    )
  ,
    ( "arrayOf"
    , Hedgehog.property $ do
        Expr.SomeNonArray gen <- Hedgehog.forAll Expr.genSomeNonArray
        xs <- Hedgehog.forAll $ Gen.list (Range.linearFrom 0 0 10) gen
        result <-
          Hedgehog.evalM $
            runWoobat $
              select $
                pure $ arrayOf $ values $ value <$> xs
        result Hedgehog.=== [xs]
    )
  ,
    ( "select spec"
    , Hedgehog.withTests 1000 $
        Hedgehog.property $ do
          SomeSelectSpec select_ expected <- Hedgehog.forAll genSomeSelectSpec
          result <- Hedgehog.evalM $ runWoobat $ select select_
          List.sort result Hedgehog.=== List.sort expected
    )
  ]

-------------------------------------------------------------------------------

data SelectSpec s a where
  SelectSpec ::
    Select s a ->
    [Barbie.Result (Barbie.FromBarbie (Expr s) a Identity)] ->
    SelectSpec s a

data SomeSelectSpec s where
  SomeSelectSpec ::
    ( Show (Barbie.Result (Barbie.FromBarbie (Expr s) a Identity))
    , Eq (Barbie.Result (Barbie.FromBarbie (Expr s) a Identity))
    , Ord (Barbie.Result (Barbie.FromBarbie (Expr s) a Identity))
    , Barbie (Expr s) a
    , HKD.AllB DatabaseType (Barbie.ToBarbie (Expr s) a)
    , HKD.ConstraintsB (Barbie.ToBarbie (Expr s) a)
    , Barbie.Resultable (Barbie.FromBarbie (Expr s) a Identity)
    ) =>
    Select s a ->
    [Barbie.Result (Barbie.FromBarbie (Expr s) a Identity)] ->
    SomeSelectSpec s

instance s ~ () => Show (SomeSelectSpec s) where
  show (SomeSelectSpec sel result) = show (fst $ compile sel, result)

data Operation where
  Operation ::
    String ->
    (forall a s. Ord a => Expr s a -> Expr s a -> Expr s Bool) ->
    (forall a. Ord a => a -> a -> Bool) ->
    Operation

instance Show Operation where
  show (Operation o _ _) = o

genSelectSpec :: forall s a. DatabaseType a => Hedgehog.Gen a -> Hedgehog.Gen (SelectSpec s (Expr s a))
genSelectSpec gen =
  Gen.choice
    [ do
        x <- gen
        let sel = pure $ value x
            expected = [x]
        pure $ SelectSpec sel expected
    , do
        xs <- Gen.list (Range.linearFrom 0 0 10) gen
        let sel = values $ value <$> xs
            expected = xs
        pure $ SelectSpec sel expected
    ]

genSomeSelectSpec :: forall s. Hedgehog.Gen (SomeSelectSpec s)
genSomeSelectSpec =
  Gen.recursive
    Gen.choice
    [ do
        Expr.Some gen <- Expr.genSome
        SelectSpec sel expected <- genSelectSpec gen
        pure $ SomeSelectSpec sel expected
    ]
    [ do
        SomeSelectSpec sel1 expected1 <- genSomeSelectSpec @s
        SomeSelectSpec sel2 expected2 <- genSomeSelectSpec @s
        let sel = (,) <$> sel1 <*> sel2
            expected = (,) <$> expected1 <*> expected2
        pure $ SomeSelectSpec sel expected
    , do
        Expr.SomeNonMaybe gen <- Expr.genSomeNonMaybe
        SelectSpec sel1 expected1 <- genSelectSpec gen
        SelectSpec sel2 expected2 <- genSelectSpec gen
        Operation _ dbOp haskellOp <- genEqOperation
        let sel = do
              x <- sel1
              y <- sel2
              where_ $ dbOp x y
              pure x
            expected = do
              x <- expected1
              y <- expected2
              guard $ haskellOp x y
              pure x
        pure $ SomeSelectSpec sel expected
    , do
        Expr.SomeNonMaybe gen <- Expr.genSomeNonMaybe
        SelectSpec sel1 expected1 <- genSelectSpec gen
        SelectSpec sel2 expected2 <- genSelectSpec gen
        Operation _ dbOp haskellOp <- genEqOperation
        let sel = do
              x <- sel1
              mx <- leftJoin sel2 $ dbOp x
              pure (x, mx)
            expected = do
              x <- expected1
              mx <- leftJoinList expected2 $ haskellOp x
              pure (x, mx)
        pure $ SomeSelectSpec sel expected
    , do
        Expr.SomeIntegral gen <- Expr.genSomeIntegral
        SelectSpec sel expected <- genSelectSpec gen
        x <- gen
        Operation _ dbOp haskellOp <- genOrdOperation
        let sel' = filter_ (dbOp $ value x) sel
            expected' = filter (haskellOp x) expected
        pure $ SomeSelectSpec sel' expected'
    , do
        Expr.SomeIntegral gen1 <- Expr.genSomeIntegral
        Expr.SomeIntegral gen2 <- Expr.genSomeIntegral
        SelectSpec sel1 expected1 <- genSelectSpec gen1
        SelectSpec sel2 expected2 <- genSelectSpec gen2
        let sel' = unnest $
              arrayOf $ do
                x <- sel1
                y <- sel2
                pure $ row $ HKD.build @(Expr.TableTwo _ _) x y
            expected' = Expr.TableTwo <$> expected1 <*> expected2
        pure $ SomeSelectSpec sel' expected'
    , do
        SomeSelectSpec sel expected <- genSomeSelectSpec @s
        let sel' = pure $ exists sel
            expected' = [not $ null expected]
        pure $ SomeSelectSpec sel' expected'
    ]

genEqOperation :: Hedgehog.Gen Operation
genEqOperation =
  Gen.element
    [ Operation "==" (==.) (==)
    , Operation "/=" (/=.) (/=)
    ]

genOrdOperation :: Hedgehog.Gen Operation
genOrdOperation =
  Gen.choice
    [ genEqOperation
    , Gen.element
        [ Operation "<" (<.) (<)
        , Operation "<=" (<=.) (<=)
        , Operation ">" (>.) (>)
        , Operation ">=" (>=.) (>=)
        ]
    ]

leftJoinList :: [a] -> (a -> Bool) -> [Maybe a]
leftJoinList as on =
  case filter on as of
    [] ->
      pure Nothing
    as' ->
      Just <$> as'
