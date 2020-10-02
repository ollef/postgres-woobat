{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Database.Woobat.Barbie where

import qualified Data.Barbie as Barbie
import Data.Coerce
import Data.Functor.Product
import Data.Generic.HKD (HKD)
import qualified Data.Generic.HKD as HKD
import Database.Woobat.Expr
import Database.Woobat.Scope

newtype Singleton a f = Singleton (f a)

instance HKD.FunctorB (Singleton a) where
  bmap f (Singleton x) = Singleton $ f x

instance HKD.TraversableB (Singleton a) where
  btraverse f (Singleton x) = Singleton <$> f x

instance Barbie f () where
  type ToBarbie f () = Barbie.Unit
  type FromBarbie f () g = ()
  toBarbie () = Barbie.Unit
  unBarbie Barbie.Unit = ()
  fromBarbie Barbie.Unit = ()

class HKD.TraversableB (ToBarbie f t) => Barbie (f :: * -> *) t where
  type ToBarbie f t :: (* -> *) -> *
  type FromBarbie f t (g :: * -> *)
  toBarbie :: t -> ToBarbie f t f
  unBarbie :: ToBarbie f t f -> t -- TODO: Remove if unused
  fromBarbie :: ToBarbie f t g -> FromBarbie f t g

instance Same s t => Barbie (Expr s) (Expr t a) where
  type ToBarbie (Expr s) (Expr t a) = Singleton a
  type FromBarbie (Expr s) (Expr t a) g = g a
  toBarbie = Singleton
  unBarbie (Singleton x) = x
  fromBarbie (Singleton x) = x

instance Same s t => Barbie (Expr s) (NullableExpr t a) where
  type ToBarbie (Expr s) (NullableExpr t a) = Singleton (Nullable a)
  type FromBarbie (Expr s) (NullableExpr t a) g = g (Nullable a)
  toBarbie (NullableExpr e) = Singleton e
  unBarbie (Singleton x) = NullableExpr x
  fromBarbie (Singleton x) = x

instance Same s t => Barbie (AggregateExpr s) (AggregateExpr t a) where
  type ToBarbie (AggregateExpr s) (AggregateExpr t a) = Singleton a
  type FromBarbie (AggregateExpr s) (AggregateExpr t a) g = g a
  toBarbie = Singleton
  unBarbie (Singleton x) = x
  fromBarbie (Singleton x) = x

instance HKD.TraversableB (HKD table) => Barbie f (HKD table f) where
  type ToBarbie f (HKD table f) = HKD table
  type FromBarbie f (HKD table f) g = HKD table g
  toBarbie = id
  unBarbie = id
  fromBarbie = id

instance (Barbie f a, Barbie f b) => Barbie f (a, b) where
  type ToBarbie f (a, b) = Product (ToBarbie f a) (ToBarbie f b)
  type FromBarbie f (a, b) g = (FromBarbie f a g, FromBarbie f b g)
  toBarbie (a, b) = Pair (toBarbie a) (toBarbie b)
  unBarbie (Pair a b) = (unBarbie a, unBarbie b)
  fromBarbie (Pair a b) = (fromBarbie @f @a a, fromBarbie @f @b b)

instance (Barbie f a, Barbie f b, Barbie f c) => Barbie f (a, b, c) where
  type ToBarbie f (a, b, c) = Product (Product (ToBarbie f a) (ToBarbie f b)) (ToBarbie f c)
  type FromBarbie f (a, b, c) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g)
  toBarbie (a, b, c) = Pair (Pair (toBarbie a) (toBarbie b)) (toBarbie c)
  unBarbie (Pair (Pair a b) c) = (unBarbie a, unBarbie b, unBarbie c)
  fromBarbie (Pair (Pair a b) c) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c)

instance (Barbie f a, Barbie f b, Barbie f c, Barbie f d) => Barbie f (a, b, c, d) where
  type ToBarbie f (a, b, c, d) = Product (Product (ToBarbie f a) (ToBarbie f b)) (Product (ToBarbie f c) (ToBarbie f d))
  type FromBarbie f (a, b, c, d) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g, FromBarbie f d g)
  toBarbie (a, b, c, d) = Pair (Pair (toBarbie a) (toBarbie b)) (Pair (toBarbie c) (toBarbie d))
  unBarbie (Pair (Pair a b) (Pair c d)) = (unBarbie a, unBarbie b, unBarbie c, unBarbie d)
  fromBarbie (Pair (Pair a b) (Pair c d)) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c, fromBarbie @f @d d)

instance (Barbie f a, Barbie f b, Barbie f c, Barbie f d, Barbie f e) => Barbie f (a, b, c, d, e) where
  type ToBarbie f (a, b, c, d, e) = Product (Product (Product (ToBarbie f a) (ToBarbie f b)) (Product (ToBarbie f c) (ToBarbie f d))) (ToBarbie f e)
  type FromBarbie f (a, b, c, d, e) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g, FromBarbie f d g, FromBarbie f e g)
  toBarbie (a, b, c, d, e) = Pair (Pair (Pair (toBarbie a) (toBarbie b)) (Pair (toBarbie c) (toBarbie d))) (toBarbie e)
  unBarbie (Pair (Pair (Pair a b) (Pair c d)) e) = (unBarbie a, unBarbie b, unBarbie c, unBarbie d, unBarbie e)
  fromBarbie (Pair (Pair (Pair a b) (Pair c d)) e) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c, fromBarbie @f @d d, fromBarbie @f @e e)

instance (Barbie f a, Barbie f b, Barbie f c, Barbie f d, Barbie f e, Barbie f a1) => Barbie f (a, b, c, d, e, a1) where
  type ToBarbie f (a, b, c, d, e, a1) = Product (Product (Product (ToBarbie f a) (ToBarbie f b)) (Product (ToBarbie f c) (ToBarbie f d))) (Product (ToBarbie f e) (ToBarbie f a1))
  type FromBarbie f (a, b, c, d, e, a1) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g, FromBarbie f d g, FromBarbie f e g, FromBarbie f a1 g)
  toBarbie (a, b, c, d, e, a1) = Pair (Pair (Pair (toBarbie a) (toBarbie b)) (Pair (toBarbie c) (toBarbie d))) (Pair (toBarbie e) (toBarbie a1))
  unBarbie (Pair (Pair (Pair a b) (Pair c d)) (Pair e a1)) = (unBarbie a, unBarbie b, unBarbie c, unBarbie d, unBarbie e, unBarbie a1)
  fromBarbie (Pair (Pair (Pair a b) (Pair c d)) (Pair e a1)) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c, fromBarbie @f @d d, fromBarbie @f @e e, fromBarbie @f @a1 a1)

instance (Barbie f a, Barbie f b, Barbie f c, Barbie f d, Barbie f e, Barbie f a1, Barbie f b1) => Barbie f (a, b, c, d, e, a1, b1) where
  type ToBarbie f (a, b, c, d, e, a1, b1) = Product (Product (Product (ToBarbie f a) (ToBarbie f b)) (Product (ToBarbie f c) (ToBarbie f d))) (Product (Product (ToBarbie f e) (ToBarbie f a1)) (ToBarbie f b1))
  type FromBarbie f (a, b, c, d, e, a1, b1) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g, FromBarbie f d g, FromBarbie f e g, FromBarbie f a1 g, FromBarbie f b1 g)
  toBarbie (a, b, c, d, e, a1, b1) = Pair (Pair (Pair (toBarbie a) (toBarbie b)) (Pair (toBarbie c) (toBarbie d))) (Pair (Pair (toBarbie e) (toBarbie a1)) (toBarbie b1))
  unBarbie (Pair (Pair (Pair a b) (Pair c d)) (Pair (Pair e a1) b1)) = (unBarbie a, unBarbie b, unBarbie c, unBarbie d, unBarbie e, unBarbie a1, unBarbie b1)
  fromBarbie (Pair (Pair (Pair a b) (Pair c d)) (Pair (Pair e a1) b1)) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c, fromBarbie @f @d d, fromBarbie @f @e e, fromBarbie @f @a1 a1, fromBarbie @f @b1 b1)

instance (Barbie f a, Barbie f b, Barbie f c, Barbie f d, Barbie f e, Barbie f a1, Barbie f b1, Barbie f c1) => Barbie f (a, b, c, d, e, a1, b1, c1) where
  type ToBarbie f (a, b, c, d, e, a1, b1, c1) = Product (Product (Product (ToBarbie f a) (ToBarbie f b)) (Product (ToBarbie f c) (ToBarbie f d))) (Product (Product (ToBarbie f e) (ToBarbie f a1)) (Product (ToBarbie f b1) (ToBarbie f c1)))
  type FromBarbie f (a, b, c, d, e, a1, b1, c1) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g, FromBarbie f d g, FromBarbie f e g, FromBarbie f a1 g, FromBarbie f b1 g, FromBarbie f c1 g)
  toBarbie (a, b, c, d, e, a1, b1, c1) = Pair (Pair (Pair (toBarbie a) (toBarbie b)) (Pair (toBarbie c) (toBarbie d))) (Pair (Pair (toBarbie e) (toBarbie a1)) (Pair (toBarbie b1) (toBarbie c1)))
  unBarbie (Pair (Pair (Pair a b) (Pair c d)) (Pair (Pair e a1) (Pair b1 c1))) = (unBarbie a, unBarbie b, unBarbie c, unBarbie d, unBarbie e, unBarbie a1, unBarbie b1, unBarbie c1)
  fromBarbie (Pair (Pair (Pair a b) (Pair c d)) (Pair (Pair e a1) (Pair b1 c1))) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c, fromBarbie @f @d d, fromBarbie @f @e e, fromBarbie @f @a1 a1, fromBarbie @f @b1 b1, fromBarbie @f @c1 c1)

-------------------------------------------------------------------------------

type ToOuter s a = FromBarbie (Expr (Inner s)) a (Expr s)

toOuter ::
  forall s a.
  (Barbie (Expr (Inner s)) a) =>
  ToBarbie (Expr (Inner s)) a (Expr (Inner s)) ->
  ToOuter s a
toOuter =
  fromBarbie @(Expr (Inner s)) @a
    . HKD.bmap (coerce :: forall x. Expr (Inner s) x -> Expr s x)

type ToLeft s a = FromBarbie (Expr (Inner s)) a (NullableExpr s)

toLeft ::
  forall s a.
  (Barbie (Expr (Inner s)) a) =>
  ToBarbie (Expr (Inner s)) a (Expr (Inner s)) ->
  ToLeft s a
toLeft =
  fromBarbie @(Expr (Inner s)) @a
    . HKD.bmap (coerce :: forall x. Expr (Inner s) x -> NullableExpr s x)

type FromAggregate s a = FromBarbie (AggregateExpr (Inner s)) a (Expr s)

fromAggregate ::
  forall s a.
  (Barbie (AggregateExpr (Inner s)) a) =>
  ToBarbie (AggregateExpr (Inner s)) a (Expr (Inner s)) ->
  FromAggregate s a
fromAggregate =
  fromBarbie @(AggregateExpr (Inner s)) @a
    . HKD.bmap (coerce :: forall x. Expr (Inner s) x -> Expr s x)
