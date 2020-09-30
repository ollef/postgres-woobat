{-# language GeneralizedNewtypeDeriving #-}
{-# language OverloadedStrings #-}
module Database.Woobat.Select.Builder where

import Control.Monad.State
import Data.ByteString (ByteString)
import Data.String
import qualified Database.Woobat.Raw as Raw
import Data.HashMap.Lazy (HashMap)
import qualified Data.HashMap.Lazy as HashMap

newtype Select s a = Select (State SelectState a)
  deriving (Functor, Applicative, Monad)

data SelectState = SelectState
  { usedNames :: !(HashMap ByteString Int)
  , rawSelect :: !Raw.Select
  }

freshNameWithSuggestion :: ByteString -> State SelectState ByteString
freshNameWithSuggestion suggestion = do
  used <- gets usedNames
  let
    count = HashMap.lookupDefault 0 suggestion used
  modify $ \s -> s { usedNames = HashMap.insert suggestion (count + 1) used }
  pure $
    if count == 0 then
      suggestion
    else
      suggestion <> "_" <> fromString (show count)

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
