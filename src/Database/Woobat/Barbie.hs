{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}

module Database.Woobat.Barbie where

import qualified Data.Barbie as Barbie
import qualified Data.Barbie.Constraints as Barbie
import Data.Functor.Identity
import qualified Data.Functor.Product as Functor
import Data.Generic.HKD (HKD)
import qualified Data.Generic.HKD as HKD
import Database.Woobat.Expr.Types
import GHC.Generics

-------------------------------------------------------------------------------

newtype Singleton (a :: *) f = Singleton (f a)
  deriving (Eq, Ord, Show, Read, Generic, Semigroup, Monoid)

instance HKD.FunctorB (Singleton a)

instance HKD.TraversableB (Singleton a)

instance HKD.ConstraintsB (Singleton a)

-------------------------------------------------------------------------------

data Product f g a = Product (f a) (g a)
  deriving (Eq, Ord, Show, Read, Generic)

instance (Semigroup (f a), Semigroup (g a)) => Semigroup (Product f g a) where
  Product fa1 ga1 <> Product fa2 ga2 = Product (fa1 <> fa2) (ga1 <> ga2)

instance (Monoid (f a), Monoid (g a)) => Monoid (Product f g a) where
  mempty = Product mempty mempty

instance (HKD.FunctorB f, HKD.FunctorB g) => HKD.FunctorB (Product f g) where
  bmap f (Product x y) = Product (HKD.bmap f x) (HKD.bmap f y)

instance (HKD.TraversableB f, HKD.TraversableB g) => HKD.TraversableB (Product f g) where
  btraverse f (Product x y) = Product <$> HKD.btraverse f x <*> HKD.btraverse f y

instance (HKD.ConstraintsB f, HKD.ConstraintsB g) => HKD.ConstraintsB (Product f g) where
  type AllB c (Product f g) = (HKD.AllB c f, HKD.AllB c g)
  baddDicts (Product x y) = Product (HKD.baddDicts x) (HKD.baddDicts y)

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
    NullableHKD hkd (Functor.Product (Barbie.Dict c) f)
  baddDicts (NullableHKD hkd) =
    NullableHKD $
      HKD.bmap (\(Functor.Pair Barbie.Dict (NullableF a)) -> NullableF (Functor.Pair Barbie.Dict a)) $
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

instance Barbie Expr (Singleton a Expr) where
  type ToBarbie Expr (Singleton a Expr) = Singleton a
  type FromBarbie Expr (Singleton a Expr) g = g a
  toBarbie = id
  fromBarbie (Singleton x) = x

instance Barbie Expr (Expr a) where
  type ToBarbie Expr (Expr a) = Singleton a
  type FromBarbie Expr (Expr a) g = g a
  toBarbie = Singleton
  fromBarbie (Singleton x) = x

instance Barbie Expr (NullableF Expr a) where
  type ToBarbie Expr (NullableF Expr a) = Singleton (Nullable a)
  type FromBarbie Expr (NullableF Expr a) g = g (Nullable a)
  toBarbie (NullableF x) = Singleton x
  fromBarbie (Singleton x) = x

instance Barbie AggregateExpr (AggregateExpr a) where
  type ToBarbie AggregateExpr (AggregateExpr a) = Singleton a
  type FromBarbie AggregateExpr (AggregateExpr a) g = g a
  toBarbie = Singleton
  fromBarbie (Singleton x) = x

instance (HKD.TraversableB (HKD table)) => Barbie Expr (HKD table Expr) where
  type ToBarbie Expr (HKD table Expr) = HKD table
  type FromBarbie Expr (HKD table Expr) g = HKD table g
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
  toBarbie (a, b) = Product (toBarbie a) (toBarbie b)
  fromBarbie (Product a b) = (fromBarbie @f @a a, fromBarbie @f @b b)

instance (Barbie f a, Barbie f b, Barbie f c) => Barbie f (a, b, c) where
  type ToBarbie f (a, b, c) = Product (Product (ToBarbie f a) (ToBarbie f b)) (ToBarbie f c)
  type FromBarbie f (a, b, c) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g)
  toBarbie (a, b, c) = Product (Product (toBarbie a) (toBarbie b)) (toBarbie c)
  fromBarbie (Product (Product a b) c) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c)

instance (Barbie f a, Barbie f b, Barbie f c, Barbie f d) => Barbie f (a, b, c, d) where
  type ToBarbie f (a, b, c, d) = Product (Product (ToBarbie f a) (ToBarbie f b)) (Product (ToBarbie f c) (ToBarbie f d))
  type FromBarbie f (a, b, c, d) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g, FromBarbie f d g)
  toBarbie (a, b, c, d) = Product (Product (toBarbie a) (toBarbie b)) (Product (toBarbie c) (toBarbie d))
  fromBarbie (Product (Product a b) (Product c d)) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c, fromBarbie @f @d d)

instance (Barbie f a, Barbie f b, Barbie f c, Barbie f d, Barbie f e) => Barbie f (a, b, c, d, e) where
  type ToBarbie f (a, b, c, d, e) = Product (Product (Product (ToBarbie f a) (ToBarbie f b)) (Product (ToBarbie f c) (ToBarbie f d))) (ToBarbie f e)
  type FromBarbie f (a, b, c, d, e) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g, FromBarbie f d g, FromBarbie f e g)
  toBarbie (a, b, c, d, e) = Product (Product (Product (toBarbie a) (toBarbie b)) (Product (toBarbie c) (toBarbie d))) (toBarbie e)
  fromBarbie (Product (Product (Product a b) (Product c d)) e) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c, fromBarbie @f @d d, fromBarbie @f @e e)

instance (Barbie f a, Barbie f b, Barbie f c, Barbie f d, Barbie f e, Barbie f a1) => Barbie f (a, b, c, d, e, a1) where
  type ToBarbie f (a, b, c, d, e, a1) = Product (Product (Product (ToBarbie f a) (ToBarbie f b)) (Product (ToBarbie f c) (ToBarbie f d))) (Product (ToBarbie f e) (ToBarbie f a1))
  type FromBarbie f (a, b, c, d, e, a1) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g, FromBarbie f d g, FromBarbie f e g, FromBarbie f a1 g)
  toBarbie (a, b, c, d, e, a1) = Product (Product (Product (toBarbie a) (toBarbie b)) (Product (toBarbie c) (toBarbie d))) (Product (toBarbie e) (toBarbie a1))
  fromBarbie (Product (Product (Product a b) (Product c d)) (Product e a1)) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c, fromBarbie @f @d d, fromBarbie @f @e e, fromBarbie @f @a1 a1)

instance (Barbie f a, Barbie f b, Barbie f c, Barbie f d, Barbie f e, Barbie f a1, Barbie f b1) => Barbie f (a, b, c, d, e, a1, b1) where
  type ToBarbie f (a, b, c, d, e, a1, b1) = Product (Product (Product (ToBarbie f a) (ToBarbie f b)) (Product (ToBarbie f c) (ToBarbie f d))) (Product (Product (ToBarbie f e) (ToBarbie f a1)) (ToBarbie f b1))
  type FromBarbie f (a, b, c, d, e, a1, b1) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g, FromBarbie f d g, FromBarbie f e g, FromBarbie f a1 g, FromBarbie f b1 g)
  toBarbie (a, b, c, d, e, a1, b1) = Product (Product (Product (toBarbie a) (toBarbie b)) (Product (toBarbie c) (toBarbie d))) (Product (Product (toBarbie e) (toBarbie a1)) (toBarbie b1))
  fromBarbie (Product (Product (Product a b) (Product c d)) (Product (Product e a1) b1)) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c, fromBarbie @f @d d, fromBarbie @f @e e, fromBarbie @f @a1 a1, fromBarbie @f @b1 b1)

instance (Barbie f a, Barbie f b, Barbie f c, Barbie f d, Barbie f e, Barbie f a1, Barbie f b1, Barbie f c1) => Barbie f (a, b, c, d, e, a1, b1, c1) where
  type ToBarbie f (a, b, c, d, e, a1, b1, c1) = Product (Product (Product (ToBarbie f a) (ToBarbie f b)) (Product (ToBarbie f c) (ToBarbie f d))) (Product (Product (ToBarbie f e) (ToBarbie f a1)) (Product (ToBarbie f b1) (ToBarbie f c1)))
  type FromBarbie f (a, b, c, d, e, a1, b1, c1) g = (FromBarbie f a g, FromBarbie f b g, FromBarbie f c g, FromBarbie f d g, FromBarbie f e g, FromBarbie f a1 g, FromBarbie f b1 g, FromBarbie f c1 g)
  toBarbie (a, b, c, d, e, a1, b1, c1) = Product (Product (Product (toBarbie a) (toBarbie b)) (Product (toBarbie c) (toBarbie d))) (Product (Product (toBarbie e) (toBarbie a1)) (Product (toBarbie b1) (toBarbie c1)))
  fromBarbie (Product (Product (Product a b) (Product c d)) (Product (Product e a1) (Product b1 c1))) = (fromBarbie @f @a a, fromBarbie @f @b b, fromBarbie @f @c c, fromBarbie @f @d d, fromBarbie @f @e e, fromBarbie @f @a1 a1, fromBarbie @f @b1 b1, fromBarbie @f @c1 c1)

-------------------------------------------------------------------------------

type Left a = FromBarbie Expr a (NullableF Expr)
type Aggregated a = FromBarbie AggregateExpr a Expr

aggregated ::
  forall a.
  (Barbie AggregateExpr a) =>
  ToBarbie AggregateExpr a Expr ->
  Aggregated a
aggregated =
  fromBarbie @AggregateExpr @a

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
