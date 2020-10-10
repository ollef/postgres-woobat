{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Exception.Safe
import qualified Data.ByteString as ByteString
import Database.Woobat
import qualified Expr
import qualified Hedgehog
import qualified Hedgehog.Main as Hedgehog
import qualified Select

main :: IO ()
main = do
  bracket
    ( createDefaultEnvironment $
        ByteString.intercalate
          " "
          [ "host=localhost"
          , "port=5432"
          , "user=woobat"
          , "dbname=woobat"
          , "client_encoding=UTF8"
          ]
    )
    destroyEnvironment
    $ \env -> do
      let runWoobat = runWoobatTWithEnvironment env
      Hedgehog.defaultMain
        [ Hedgehog.checkParallel $ Hedgehog.Group "Select" $ Select.properties runWoobat
        , Hedgehog.checkParallel $ Hedgehog.Group "Expr" $ Expr.properties runWoobat
        ]
