{-# language AllowAmbiguousTypes #-}
{-# language FlexibleContexts #-}
{-# language FlexibleInstances #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language TypeApplications #-}
{-# language TypeFamilies #-}
{-# language TypeSynonymInstances #-}
module Database.Woobat.Compiler where

import Control.Applicative
import Control.Monad.State
import Data.Barbie (TraversableB, bfoldMap)
import Data.ByteString (ByteString)
import Data.Generic.HKD (HKD)
import Data.Text (Text)
import Database.Woobat.Expr
import Database.Woobat.Query.Syntax
import qualified Database.Woobat.Raw as Raw
import GHC.Generics

type Compiler = State CompilerState

data CompilerState = CompilerState
  { nextName :: !Int
  , parameters :: [Raw.Param]
  }

type family Result r where
  Result (Expr s a) = a
  Result (Row s table) = table
  Result (a, b) = (Result a, Result b)

newtype ResultParser a = ResultParser (StateT [Maybe ByteString] (Either Text) a)
  deriving (Functor, Applicative, Monad)

class FromResult a where
  resultParser :: ResultParser (Result a)

class DatabaseResult a where
  databaseResult :: a -> [Raw.SQL]

instance DatabaseResult (Expr s a) where
  databaseResult (Expr sql) = [sql]

instance TraversableB (HKD table) => DatabaseResult (Row s table) where
  databaseResult = bfoldMap databaseResult

instance (DatabaseResult a, DatabaseResult b) => DatabaseResult (a, b) where
  databaseResult (a, b) = databaseResult a <> databaseResult b

compileQuery :: DatabaseResult a => Query s result -> Compiler Raw.SQL
compileQuery (Pure result) = _

