{-# LANGUAGE OverloadedStrings #-}

module Database.Woobat.Query.Monad where

import Data.ByteString (ByteString)
import Data.HashMap.Lazy (HashMap)
import qualified Data.HashMap.Lazy as HashMap
import Data.String
import qualified Database.Woobat.Raw as Raw

class Monad m => MonadQuery m where
  getUsedNames :: m (HashMap ByteString Int)
  putUsedNames :: HashMap ByteString Int -> m ()
  getFrom :: m (Raw.From ())
  putFrom :: Raw.From () -> m ()
  addWhere :: Raw.Expr -> m ()

addFrom :: MonadQuery m => Raw.From () -> m ()
addFrom from' = do
  from <- getFrom
  putFrom $ from <> from'

freshName :: MonadQuery m => ByteString -> m ByteString
freshName suggestion = do
  used <- getUsedNames
  let count = HashMap.lookupDefault 0 suggestion used
  putUsedNames $ HashMap.insert suggestion (count + 1) used
  pure $
    if count == 0
      then suggestion
      else suggestion <> "_" <> fromString (show count)
