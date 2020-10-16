{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.Woobat.Select.Builder where

import Control.Monad.State
import Data.ByteString (ByteString)
import Data.HashMap.Lazy (HashMap)
import Database.Woobat.Query.Monad
import qualified Database.Woobat.Raw as Raw

newtype Select s a = Select (State SelectState a)
  deriving (Functor, Applicative, Monad)

data SelectState = SelectState
  { usedNames :: !(HashMap ByteString Int)
  , rawSelect :: !Raw.Select
  }

instance MonadQuery Select where
  getUsedNames =
    Select $
      gets usedNames
  putUsedNames usedNames_ = Select $
    modify $ \s -> s {usedNames = usedNames_}
  getFrom =
    Select $ gets $ Raw.from . rawSelect
  putFrom f =
    Select $ modify $ \s -> s {rawSelect = (rawSelect s) {Raw.from = f}}
  addWhere (Raw.Expr where_) = Select $
    modify $ \s -> s {rawSelect = rawSelect s <> mempty {Raw.wheres = pure $ where_ $ usedNames s}}

run :: HashMap ByteString Int -> Select s a -> (a, SelectState)
run used (Select s) =
  runState s SelectState {usedNames = used, rawSelect = mempty}

addSelect :: Raw.Select -> State SelectState ()
addSelect sel =
  modify $ \s -> s {rawSelect = rawSelect s <> sel}

subquery :: MonadQuery query => State SelectState a -> query s (a, Raw.Select)
subquery q = do
  used <- getUsedNames
  let (result, st) = run used $ Select q
  putUsedNames $ usedNames st
  pure (result, rawSelect st)
