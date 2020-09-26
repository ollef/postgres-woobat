{-# language GeneralizedNewtypeDeriving #-}
{-# language OverloadedStrings #-}
module Database.Woobat.Select.Builder where

import Control.Monad.State
import Data.ByteString (ByteString)
import Data.String
import qualified Database.Woobat.Raw as Raw

newtype Select s a = Select (State SelectState a)
  deriving (Functor, Applicative, Monad)

data SelectState = SelectState
  { nextName :: !Int
  , rawSelect :: !Raw.Select
  }

freshNameWithSuggestion :: ByteString -> State SelectState ByteString
freshNameWithSuggestion suggestion = do
  n <- gets nextName
  modify $ \s -> s { nextName = n + 1 }
  pure $ suggestion <> "_" <> fromString (show n)

subquery :: State SelectState a -> State SelectState (Raw.Select, a)
subquery q = do
  before <- gets rawSelect
  modify $ \s -> s { rawSelect = mempty }
  result <- q
  after <- gets rawSelect
  modify $ \s -> s { rawSelect = before }
  pure (after, result)

addSelect :: Raw.Select -> State SelectState ()
addSelect sel =
  modify $ \s -> s { rawSelect = rawSelect s <> sel }
