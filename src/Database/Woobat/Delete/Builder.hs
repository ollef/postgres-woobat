{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.Woobat.Delete.Builder where

import Control.Monad.State
import Data.ByteString (ByteString)
import Data.HashMap.Lazy (HashMap)
import Database.Woobat.Query.Monad
import qualified Database.Woobat.Raw as Raw

newtype Delete s a = Delete (State DeleteState a)
  deriving (Functor, Applicative, Monad)

data DeleteState = DeleteState
  { usedNames :: !(HashMap ByteString Int)
  , rawFrom :: !(Raw.From ())
  , wheres :: Raw.Tsil Raw.SQL
  }

instance MonadQuery Delete where
  getUsedNames =
    Delete $ gets usedNames
  putUsedNames usedNames_ =
    Delete $ modify $ \s -> s {usedNames = usedNames_}
  getFrom =
    Delete $ gets rawFrom
  putFrom f =
    Delete $ modify $ \s -> s {rawFrom = f}
  addWhere (Raw.Expr where_) =
    Delete $ modify $ \s -> s {wheres = pure $ where_ $ usedNames s}

run :: HashMap ByteString Int -> Delete s a -> (a, DeleteState)
run used (Delete s) =
  runState s DeleteState {usedNames = used, rawFrom = mempty, wheres = mempty}
