{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.Woobat.Update.Builder where

import Control.Monad.State
import Data.ByteString (ByteString)
import Data.HashMap.Lazy (HashMap)
import Database.Woobat.Query.Monad
import qualified Database.Woobat.Raw as Raw

newtype Update s a = Update (State UpdateState a)
  deriving (Functor, Applicative, Monad)

data UpdateState = UpdateState
  { usedNames :: !(HashMap ByteString Int)
  , rawFrom :: !(Raw.From ())
  , wheres :: Raw.Tsil Raw.SQL
  }

instance MonadQuery Update where
  getUsedNames =
    Update $ gets usedNames
  putUsedNames usedNames_ =
    Update $ modify $ \s -> s {usedNames = usedNames_}
  getFrom =
    Update $ gets rawFrom
  putFrom f =
    Update $ modify $ \s -> s {rawFrom = f}
  addWhere (Raw.Expr where_) =
    Update $ modify $ \s -> s {wheres = pure $ where_ $ usedNames s}

run :: HashMap ByteString Int -> Update s a -> (a, UpdateState)
run used (Update s) =
  runState s UpdateState {usedNames = used, rawFrom = mempty, wheres = mempty}
