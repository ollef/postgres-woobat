{-# LANGUAGE OverloadedStrings #-}

module Main where

import qualified Expr
import qualified Hedgehog
import qualified Hedgehog.Main as Hedgehog

main :: IO ()
main =
  Hedgehog.defaultMain
    [Hedgehog.checkParallel $ Hedgehog.Group "Exprs" Expr.properties]
