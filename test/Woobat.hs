{-# LANGUAGE OverloadedStrings #-}

module Woobat where

import Control.Monad.Reader
import qualified Data.ByteString as ByteString
import Database.Woobat

runWoobat :: MonadIO m => Woobat a -> m a
runWoobat =
  liftIO
    . runWoobatT
      ( ByteString.intercalate
          " "
          [ "host=localhost"
          , "port=5432"
          , "user=woobat"
          , "dbname=woobat"
          , "client_encoding=UTF8"
          ]
      )
