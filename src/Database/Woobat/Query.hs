{-# language PatternSynonyms #-}
module Database.Woobat.Query
  ( Query
  , module Database.Woobat.Query
  , module Export
  ) where

import Database.Woobat.Compiler
import Database.Woobat.Expr
import Database.Woobat.Monad
import Database.Woobat.Query.Syntax
import Database.Woobat.Query.Syntax as Export hiding (Query)
import Database.Woobat.Scope
import Database.Woobat.Table

pattern (:*:) :: a -> b -> (a, b)
pattern a :*: b = (a, b)

infixr 1 :*:

query :: MonadWoobat m => Query s a -> m [Result a]
query = undefined

select :: Table table -> Query s (Row s table)
select table = Select table pure

where_ :: Same s t => Expr s Bool -> Query t ()
where_ cond = Where cond $ pure ()

filter_ :: (Same s t, Same t u) => (a -> Expr s Bool) -> Query t a -> Query u a
filter_ f q = do
  a <- q
  where_ $ f a
  pure a

order :: Same s t => Expr s a -> Order -> Query t ()
order expr order_ = Order expr order_ $ pure ()

innerJoin :: (Same s t, Same t u) => Query (Inner s) a -> (Outer a -> Expr t Bool) -> Query u (Outer a)
innerJoin q on = InnerJoin q on pure

leftJoin :: (Same s t, Same t u) => Query (Inner s) a -> (Outer a -> Expr t Bool) -> Query u (Left a)
leftJoin q on = LeftJoin q on pure

aggregate :: Same s t => Query (Inner s) a -> Query t (Aggregate a)
aggregate q = Aggregate q pure

groupBy :: (Same s t, Same t u) => Expr (Inner s) a -> Query (Inner t) (AggregateExpr (Inner u) a)
groupBy expr = GroupBy expr pure
