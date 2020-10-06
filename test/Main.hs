{-# LANGUAGE OverloadedStrings #-}

module Main where

import qualified Expr
import qualified Hedgehog
import qualified Hedgehog.Main as Hedgehog
import qualified Select

main :: IO ()
main =
  Hedgehog.defaultMain
    [ Hedgehog.checkParallel $ Hedgehog.Group "Select" Select.properties
    , Hedgehog.checkParallel $ Hedgehog.Group "Expr" Expr.properties
    ]
