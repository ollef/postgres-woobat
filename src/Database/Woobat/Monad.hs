{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE UndecidableInstances #-}

module Database.Woobat.Monad where

import Control.Exception.Safe
import Control.Monad.IO.Class
import Control.Monad.Reader
import Control.Monad.Trans.Control (MonadBaseControl)
import Data.ByteString (ByteString)
import Data.Pool (Pool)
import qualified Data.Pool as Pool
import qualified Database.PostgreSQL.LibPQ as LibPQ

class MonadIO m => MonadWoobat m where
  withConnection :: (LibPQ.Connection -> m a) -> m a
  transactUsing :: LibPQ.Connection -> m a -> m a

type Woobat = WoobatT IO

newtype WoobatT m a = WoobatT {unWoobatT :: ReaderT Environment m a}
  deriving newtype (Functor, Applicative, Monad, MonadIO)

data Environment = Environment
  { connectionPool :: !(Pool LibPQ.Connection)
  , ongoingTransaction :: !(Maybe LibPQ.Connection)
  }

data ConnectionError = ConnectionError LibPQ.ConnStatus
  deriving (Show, Exception)

data ExecutionError = ExecutionError LibPQ.ExecStatus (Maybe ByteString)
  deriving (Show, Exception)

createDefaultEnvironment :: MonadIO m => ByteString -> m Environment
createDefaultEnvironment connectionInfo = do
  pool <-
    liftIO $
      Pool.createPool
        (connect connectionInfo)
        LibPQ.finish
        1 -- stripes
        60 -- unused connections are kept open for a minute
        10 -- max. 10 connections open per stripe
  pure
    Environment
      { connectionPool = pool
      , ongoingTransaction = Nothing
      }

connect :: (MonadMask m, MonadIO m) => ByteString -> m LibPQ.Connection
connect connectionInfo =
  bracketOnError (liftIO $ LibPQ.connectdb connectionInfo) close $ \connection -> do
    status <- liftIO $ LibPQ.status connection
    case status of
      LibPQ.ConnectionOk -> do
        void $ liftIO $ LibPQ.exec connection "SET client_min_messages TO WARNING;"
        return connection
      _ -> throwM $ ConnectionError status

close :: MonadIO m => LibPQ.Connection -> m ()
close = liftIO . LibPQ.finish

instance (MonadIO m, MonadBaseControl IO m) => MonadWoobat (WoobatT m) where
  withConnection k = WoobatT $ do
    env <- ask
    case ongoingTransaction env of
      Nothing ->
        Pool.withResource (connectionPool env) (unWoobatT . k)
      Just connection ->
        unWoobatT $ k connection

  transactUsing connection (WoobatT k) =
    WoobatT $ local (\env -> env {ongoingTransaction = Just connection}) k

transaction :: (MonadMask m, MonadWoobat m) => m a -> m a
transaction k =
  withConnection $ \connection ->
    bracketWithError
      (liftIO $ LibPQ.exec connection "BEGIN")
      ( \maybeException _ -> liftIO $
          LibPQ.exec connection $ case maybeException of
            Nothing -> "COMMIT"
            Just _ -> "ROLLBACK"
      )
      (\_ -> transactUsing connection k)
