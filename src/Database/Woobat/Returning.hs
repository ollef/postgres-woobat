{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}

module Database.Woobat.Returning where

import Data.Functor.Identity
import qualified Data.Generic.HKD as HKD
import Database.Woobat.Barbie
import Database.Woobat.Expr

data Returning a where
  ReturningNothing :: Returning ()
  Returning ::
    ( Barbie (Expr ()) a
    , HKD.AllB DatabaseType (ToBarbie (Expr ()) a)
    , HKD.ConstraintsB (ToBarbie (Expr ()) a)
    , Resultable (FromBarbie (Expr ()) a Identity)
    ) =>
    a ->
    Returning [Result (FromBarbie (Expr ()) a Identity)]
  ReturningRowCount :: Returning Int

returningNothing :: a -> Returning ()
returningNothing = const ReturningNothing

returning ::
  ( Barbie (Expr ()) a
  , HKD.AllB DatabaseType (ToBarbie (Expr ()) a)
  , HKD.ConstraintsB (ToBarbie (Expr ()) a)
  , Resultable (FromBarbie (Expr ()) a Identity)
  ) =>
  a ->
  Returning [Result (FromBarbie (Expr ()) a Identity)]
returning = Returning

returningRowCount :: a -> Returning Int
returningRowCount = const ReturningRowCount
