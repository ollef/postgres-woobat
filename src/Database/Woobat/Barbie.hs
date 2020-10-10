{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}

module Database.Woobat.Barbie where

import qualified Data.Barbie as Barbie
import qualified Data.Barbie.Constraints as Barbie
import Data.Coerce
import Data.Functor.Identity
import Data.Functor.Product
import Data.Generic.HKD (HKD)
import qualified Data.Generic.HKD as HKD
import Database.Woobat.Expr.Types
import Database.Woobat.Scope
import GHC.Generics

-------------------------------------------------------------------------------

newtype Singleton a f = Singleton (f a)
  deriving (Generic, Semigroup, Monoid)

instance HKD.FunctorB (Singleton a)

instance HKD.TraversableB (Singleton a)

instance HKD.ConstraintsB (Singleton a)

-------------------------------------------------------------------------------

newtype NullableHKD hkd f = NullableHKD (hkd (NullableF f))

class c (Nullable a) => ConstrainNullable c a

instance c (Nullable a) => ConstrainNullable c a

instance HKD.ConstraintsB hkd => HKD.ConstraintsB (NullableHKD hkd) where
  type AllB c (NullableHKD hkd) = Barbie.AllB (ConstrainNullable c) hkd
  baddDicts ::
    forall c f.
    HKD.AllB c (NullableHKD hkd) =>
    NullableHKD hkd f ->
    NullableHKD hkd (Product (Barbie.Dict c) f)
  baddDicts (NullableHKD hkd) =
    NullableHKD $
      HKD.bmap (\(Pair Barbie.Dict (NullableF a)) -> NullableF (Pair Barbie.Dict a)) $
        HKD.baddDicts @_ @_ @(ConstrainNullable c) hkd

instance HKD.FunctorB hkd => HKD.FunctorB (NullableHKD hkd) where
  bmap f (NullableHKD hkd) =
    NullableHKD $ HKD.bmap (\(NullableF x) -> NullableF $ f x) hkd

instance HKD.TraversableB hkd => HKD.TraversableB (NullableHKD hkd) where
  btraverse f (NullableHKD hkd) =
    NullableHKD <$> HKD.btraverse (\(NullableF x) -> NullableF <$> f x) hkd

-------------------------------------------------------------------------------

class HKD.TraversableB (ToBarbie f t) => Barbie (f :: * -> *) t where
  type ToBarbie f t :: (* -> *) -> *
  type FromBarbie f t (g :: * -> *)
  toBarbie :: t -> ToBarbie f t f
  fromBarbie :: ToBarbie f t g -> FromBarbie f t g

instance Barbie f () where
  type ToBarbie f () = Barbie.Unit
  type FromBarbie f () _ = ()
  toBarbie () = Barbie.Unit
  fromBarbie Barbie.Unit = ()

instance Same s t => Barbie (Expr s) (Singleton a (Expr t)) where
  type ToBarbie (Expr s) (Singleton a (Expr t)) = Singleton a
  type FromBarbie (Expr s) (Singleton a (Expr t)) g = g a
  toBarbie = id
  fromBarbie (Singleton x) = x

instance Same s t => Barbie (Expr s) (Expr t a) where
  type ToBarbie (Expr s) (Expr t a) = Singleton a
  type FromBarbie (Expr s) (Expr t a) g = g a
  toBarbie = Singleton
  fromBarbie (Singleton x) = x

instance Same s t => Barbie (Expr s) (NullableF (Expr t) a) where
  type ToBarbie (Expr s) (NullableF (Expr t) a) = Singleton (Nullable a)
  type FromBarbie (Expr s) (NullableF (Expr t) a) g = g (Nullable a)
  toBarbie (NullableF x) = Singleton x
  fromBarbie (Singleton x) = x

instance Same s t => Barbie (AggregateExpr s) (AggregateExpr t a) where
  type ToBarbie (AggregateExpr s) (AggregateExpr t a) = Singleton a
  type FromBarbie (AggregateExpr s) (AggregateExpr t a) g = g a
  toBarbie = Singleton
  fromBarbie (Singleton x) = x

instance (Same s t, HKD.TraversableB (HKD table)) => Barbie (Expr s) (HKD table (Expr t)) where
  type ToBarbie (Expr s) (HKD table (Expr t)) = HKD table
  type FromBarbie (Expr s) (HKD table (Expr t)) g = HKD table g
  toBarbie = id
  fromBarbie = id

instance HKD.TraversableB (HKD table) => Barbie f (HKD table (NullableF f)) where
  type ToBarbie f (HKD table (NullableF f)) = NullableHKD (HKD table)
  type FromBarbie f (HKD table (NullableF f)) g = HKD table (NullableF g)
  toBarbie = NullableHKD
  fromBarbie (NullableHKD x) = x

instance (Barbie f a, Barbie f b) => Barbie f (a, b) where
  type ToBarbie f (a, b) = Product (ToBarbie f a) (ToBarbie f b)
  type FromBarbie f (a, b) g = (FromBarbie f a g, FromBarbie f b g)
  toBarbie (a, b) = Pair (toBarbie a) (toBarbie b)
  fromBarbie (Pair a b) = (fromBarbie @f @a a, fromBarbie @f @b b)

instance (Barbie f a, Barbie f b, Barbie f c) => Barbie f (a, b, c) where
  type ToBarbie f (a, b, c) = Product (Product (ToBarbie f a) (ToBarbie f b)) (ToBarbie f c)
  type FromBarbie f (a, b, c) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g)
  toBarbie (a, b, c) = Pair (Pair (toBarbie a) (toBarbie b)) (toBarbie c)
  fromBarbie (Pair (Pair a b) c) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c)

instance (Barbie f a, Barbie f b, Barbie f c, Barbie f d) => Barbie f (a, b, c, d) where
  type ToBarbie f (a, b, c, d) = Product (Product (ToBarbie f a) (ToBarbie f b)) (Product (ToBarbie f c) (ToBarbie f d))
  type FromBarbie f (a, b, c, d) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g, FromBarbie f d g)
  toBarbie (a, b, c, d) = Pair (Pair (toBarbie a) (toBarbie b)) (Pair (toBarbie c) (toBarbie d))
  fromBarbie (Pair (Pair a b) (Pair c d)) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c, fromBarbie @f @d d)

instance (Barbie f a, Barbie f b, Barbie f c, Barbie f d, Barbie f e) => Barbie f (a, b, c, d, e) where
  type ToBarbie f (a, b, c, d, e) = Product (Product (Product (ToBarbie f a) (ToBarbie f b)) (Product (ToBarbie f c) (ToBarbie f d))) (ToBarbie f e)
  type FromBarbie f (a, b, c, d, e) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g, FromBarbie f d g, FromBarbie f e g)
  toBarbie (a, b, c, d, e) = Pair (Pair (Pair (toBarbie a) (toBarbie b)) (Pair (toBarbie c) (toBarbie d))) (toBarbie e)
  fromBarbie (Pair (Pair (Pair a b) (Pair c d)) e) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c, fromBarbie @f @d d, fromBarbie @f @e e)

instance (Barbie f a, Barbie f b, Barbie f c, Barbie f d, Barbie f e, Barbie f a1) => Barbie f (a, b, c, d, e, a1) where
  type ToBarbie f (a, b, c, d, e, a1) = Product (Product (Product (ToBarbie f a) (ToBarbie f b)) (Product (ToBarbie f c) (ToBarbie f d))) (Product (ToBarbie f e) (ToBarbie f a1))
  type FromBarbie f (a, b, c, d, e, a1) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g, FromBarbie f d g, FromBarbie f e g, FromBarbie f a1 g)
  toBarbie (a, b, c, d, e, a1) = Pair (Pair (Pair (toBarbie a) (toBarbie b)) (Pair (toBarbie c) (toBarbie d))) (Pair (toBarbie e) (toBarbie a1))
  fromBarbie (Pair (Pair (Pair a b) (Pair c d)) (Pair e a1)) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c, fromBarbie @f @d d, fromBarbie @f @e e, fromBarbie @f @a1 a1)

instance (Barbie f a, Barbie f b, Barbie f c, Barbie f d, Barbie f e, Barbie f a1, Barbie f b1) => Barbie f (a, b, c, d, e, a1, b1) where
  type ToBarbie f (a, b, c, d, e, a1, b1) = Product (Product (Product (ToBarbie f a) (ToBarbie f b)) (Product (ToBarbie f c) (ToBarbie f d))) (Product (Product (ToBarbie f e) (ToBarbie f a1)) (ToBarbie f b1))
  type FromBarbie f (a, b, c, d, e, a1, b1) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g, FromBarbie f d g, FromBarbie f e g, FromBarbie f a1 g, FromBarbie f b1 g)
  toBarbie (a, b, c, d, e, a1, b1) = Pair (Pair (Pair (toBarbie a) (toBarbie b)) (Pair (toBarbie c) (toBarbie d))) (Pair (Pair (toBarbie e) (toBarbie a1)) (toBarbie b1))
  fromBarbie (Pair (Pair (Pair a b) (Pair c d)) (Pair (Pair e a1) b1)) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c, fromBarbie @f @d d, fromBarbie @f @e e, fromBarbie @f @a1 a1, fromBarbie @f @b1 b1)

instance (Barbie f a, Barbie f b, Barbie f c, Barbie f d, Barbie f e, Barbie f a1, Barbie f b1, Barbie f c1) => Barbie f (a, b, c, d, e, a1, b1, c1) where
  type ToBarbie f (a, b, c, d, e, a1, b1, c1) = Product (Product (Product (ToBarbie f a) (ToBarbie f b)) (Product (ToBarbie f c) (ToBarbie f d))) (Product (Product (ToBarbie f e) (ToBarbie f a1)) (Product (ToBarbie f b1) (ToBarbie f c1)))
  type FromBarbie f (a, b, c, d, e, a1, b1, c1) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g, FromBarbie f d g, FromBarbie f e g, FromBarbie f a1 g, FromBarbie f b1 g, FromBarbie f c1 g)
  toBarbie (a, b, c, d, e, a1, b1, c1) = Pair (Pair (Pair (toBarbie a) (toBarbie b)) (Pair (toBarbie c) (toBarbie d))) (Pair (Pair (toBarbie e) (toBarbie a1)) (Pair (toBarbie b1) (toBarbie c1)))
  fromBarbie (Pair (Pair (Pair a b) (Pair c d)) (Pair (Pair e a1) (Pair b1 c1))) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c, fromBarbie @f @d d, fromBarbie @f @e e, fromBarbie @f @a1 a1, fromBarbie @f @b1 b1, fromBarbie @f @c1 c1)

-------------------------------------------------------------------------------

type Outer s a = FromBarbie (Expr (Inner s)) a (Expr s)

outer ::
  forall s a.
  (Barbie (Expr (Inner s)) a) =>
  ToBarbie (Expr (Inner s)) a (Expr (Inner s)) ->
  Outer s a
outer =
  fromBarbie @(Expr (Inner s)) @a
    . HKD.bmap (coerce :: forall x. Expr (Inner s) x -> Expr s x)

type Left s a = FromBarbie (Expr (Inner s)) a (NullableF (Expr s))

left ::
  forall s a.
  (Barbie (Expr (Inner s)) a) =>
  ToBarbie (Expr (Inner s)) a (Expr (Inner s)) ->
  Left s a
left =
  fromBarbie @(Expr (Inner s)) @a
    . HKD.bmap (coerce :: forall x. Expr (Inner s) x -> NullableExpr s x)

type Aggregated s a = FromBarbie (AggregateExpr (Inner s)) a (Expr s)

aggregated ::
  forall s a.
  (Barbie (AggregateExpr (Inner s)) a) =>
  ToBarbie (AggregateExpr (Inner s)) a (Expr (Inner s)) ->
  Aggregated s a
aggregated =
  fromBarbie @(AggregateExpr (Inner s)) @a
    . HKD.bmap (coerce :: forall x. Expr (Inner s) x -> Expr s x)

-------------------------------------------------------------------------------
class Resultable a where
  type Result a
  result :: a -> Result a

instance Resultable () where
  type Result () = ()
  result = id

instance Resultable (Identity a) where
  type Result (Identity a) = a
  result = runIdentity

instance HKD.Construct Identity table => Resultable (HKD table Identity) where
  type Result (HKD table Identity) = table
  result table = runIdentity $ HKD.construct table

class FromNullable a where
  fromNullable :: Nullable a -> Maybe a

instance {-# OVERLAPPABLE #-} (Nullable a ~ Maybe a) => FromNullable a where
  fromNullable = id

instance FromNullable (Maybe a) where
  fromNullable = Just

instance (HKD.Construct Maybe table, HKD.ConstraintsB (HKD table), HKD.AllB FromNullable (HKD table)) => Resultable (HKD table (NullableF Identity)) where
  type Result (HKD table (NullableF Identity)) = Maybe table
  result table =
    HKD.construct $
      Barbie.bmapC @FromNullable (\(NullableF (Identity x)) -> fromNullable x) table

instance (Resultable a, Resultable b) => Resultable (a, b) where
  type Result (a, b) = (Result a, Result b)
  result (a, b) = (result a, result b)

instance (Resultable a, Resultable b, Resultable c) => Resultable (a, b, c) where
  type Result (a, b, c) = (Result a, Result b, Result c)
  result (a, b, c) = (result a, result b, result c)

instance (Resultable a, Resultable b, Resultable c, Resultable d) => Resultable (a, b, c, d) where
  type Result (a, b, c, d) = (Result a, Result b, Result c, Result d)
  result (a, b, c, d) = (result a, result b, result c, result d)

instance (Resultable a, Resultable b, Resultable c, Resultable d, Resultable e) => Resultable (a, b, c, d, e) where
  type Result (a, b, c, d, e) = (Result a, Result b, Result c, Result d, Result e)
  result (a, b, c, d, e) = (result a, result b, result c, result d, result e)

instance (Resultable a, Resultable b, Resultable c, Resultable d, Resultable e, Resultable f) => Resultable (a, b, c, d, e, f) where
  type Result (a, b, c, d, e, f) = (Result a, Result b, Result c, Result d, Result e, Result f)
  result (a, b, c, d, e, f) = (result a, result b, result c, result d, result e, result f)

instance (Resultable a, Resultable b, Resultable c, Resultable d, Resultable e, Resultable f, Resultable g) => Resultable (a, b, c, d, e, f, g) where
  type Result (a, b, c, d, e, f, g) = (Result a, Result b, Result c, Result d, Result e, Result f, Result g)
  result (a, b, c, d, e, f, g) = (result a, result b, result c, result d, result e, result f, result g)

instance (Resultable a, Resultable b, Resultable c, Resultable d, Resultable e, Resultable f, Resultable g, Resultable h) => Resultable (a, b, c, d, e, f, g, h) where
  type Result (a, b, c, d, e, f, g, h) = (Result a, Result b, Result c, Result d, Result e, Result f, Result g, Result h)
  result (a, b, c, d, e, f, g, h) = (result a, result b, result c, result d, result e, result f, result g, result h)
