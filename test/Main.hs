{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Exception.Safe
import Control.Monad.Reader
import qualified Data.ByteString as ByteString
import Data.IORef
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Database.PostgreSQL.LibPQ as LibPQ
import Database.Woobat
import qualified Hedgehog
import qualified Hedgehog.Main as Hedgehog

main :: IO ()
main = do
  testNumber <- newIORef 0
  Hedgehog.defaultMain
    [ Hedgehog.checkParallel $ Hedgehog.Group "Select" $ selectTests testNumber
    ]

selectTests :: IORef Int -> [(Hedgehog.PropertyName, Hedgehog.Property)]
selectTests testNumber =
  [ ("empty", empty testNumber)
  ]

woobatTest :: MonadIO m => IORef Int -> Woobat a -> m a
woobatTest testNumber m = liftIO $ do
  databaseName <- atomicModifyIORef testNumber $ \num ->
    (num + 1, Text.encodeUtf8 $ "woobat_test_" <> Text.pack (show num))
  runWoobatT
    ( ByteString.intercalate
        " "
        [ "host=localhost"
        , "port=5432"
        , "user=woobat"
        , "dbname=woobat"
        , "client_encoding=UTF8"
        ]
    )
    $ withConnection $ \connection -> do
      let exec code = do
            maybeResult <- liftIO $ LibPQ.exec connection code
            case maybeResult of
              Nothing -> throwM $ ConnectionError LibPQ.ConnectionBad
              Just result -> do
                status <- liftIO $ LibPQ.resultStatus result
                case status of
                  LibPQ.CommandOk -> return ()
                  _ -> do
                    message <- liftIO $ LibPQ.resultErrorMessage result
                    throwM $ ExecutionError status message
      exec $ "DROP DATABASE IF EXISTS " <> databaseName
      exec $ "CREATE DATABASE " <> databaseName
  result <-
    runWoobatT
      ( ByteString.intercalate
          " "
          [ "host=localhost"
          , "port=5432"
          , "user=woobat"
          , "dbname=" <> databaseName
          , "client_encoding=UTF8"
          ]
      )
      m
  runWoobatT
    ( ByteString.intercalate
        " "
        [ "host=localhost"
        , "port=5432"
        , "user=woobat"
        , "dbname=woobat"
        , "client_encoding=UTF8"
        ]
    )
    $ withConnection $ \connection -> do
      let exec code = do
            maybeResult <- liftIO $ LibPQ.exec connection code
            case maybeResult of
              Nothing -> throwM $ ConnectionError LibPQ.ConnectionBad
              Just result -> do
                status <- liftIO $ LibPQ.resultStatus result
                case status of
                  LibPQ.CommandOk -> return ()
                  _ -> do
                    message <- liftIO $ LibPQ.resultErrorMessage result
                    throwM $ ExecutionError status message
      exec $ "DROP DATABASE " <> databaseName
  return result

empty :: IORef Int -> Hedgehog.Property
empty testNumber =
  Hedgehog.property $ do
    result <- woobatTest testNumber $ select $ pure ()
    result Hedgehog.=== [()]
