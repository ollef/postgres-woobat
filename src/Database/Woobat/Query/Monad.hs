{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuantifiedConstraints #-}

module Database.Woobat.Query.Monad where

import Data.ByteString (ByteString)
import Data.HashMap.Lazy (HashMap)
import qualified Data.HashMap.Lazy as HashMap
import Data.String
import qualified Database.Woobat.Raw as Raw

class (forall s. Monad (m s)) => MonadQuery m where
  getUsedNames :: m s (HashMap ByteString Int)
  putUsedNames :: HashMap ByteString Int -> m s ()
  getFrom :: m s (Raw.From ())
  putFrom :: Raw.From () -> m s ()
  addWhere :: Raw.Expr -> m s ()

addFrom :: MonadQuery m => Raw.From () -> m s ()
addFrom from' = do
  from <- getFrom
  putFrom $ from <> from'

freshName :: MonadQuery m => ByteString -> m s ByteString
freshName suggestion = do
  used <- getUsedNames
  let count = HashMap.lookupDefault 0 suggestion used
  putUsedNames $ HashMap.insert suggestion (count + 1) used
  pure $
    if count == 0
      then suggestion
      else suggestion <> "_" <> fromString (show count)
