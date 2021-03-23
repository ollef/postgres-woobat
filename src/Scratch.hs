{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Scratch where

import qualified Barbies
import Control.Lens hiding (from)
import Data.Generic.HKD (HKD)
import qualified Data.Generic.HKD as HKD
import Data.Generics.Labels ()
import Data.Generics.Product.Fields (HasField')
import Data.Int
import Data.Text (Text)
import Database.Woobat
import GHC.Generics hiding (from)
import GHC.OverloadedLabels (IsLabel (fromLabel))

instance
  {-# OVERLAPPING #-}
  ( Functor f
  , HasField' field (HKD structure (NullableF g)) (NullableF g a)
  , na ~ Nullable a
  ) =>
  IsLabel field ((g na -> f (g na)) -> HKD structure (NullableF g) -> f (HKD structure (NullableF g)))
  where
  fromLabel = HKD.field @field . lens (\(NullableF e) -> e) const

newtype Newtype = Newtype Text
  deriving (DatabaseNewtype Text)

data Profile = Profile
  { name :: !Text
  , description :: !Text
  , age :: !Int
  , optional :: !(Maybe Text)
  , boolean :: !Bool
  , newtype_ :: !Newtype
  }
  deriving (Generic)

profile :: Table (HKD Profile)
profile = table "profile"

ppp :: Profile
ppp =
  Profile
    { name = "Olle"
    , age = 33
    , description = ""
    , optional = Nothing
    , boolean = False
    , newtype_ = Newtype "newtype"
    }

descriptionQuery :: Select (Expr Text)
descriptionQuery = do
  p <- from profile
  where_ $ row p ==. row (record ppp)
  where_ $ p ^. #name ==. "Olle"
  pure $ p ^. #description

selectDescriptionQuery :: (MonadWoobat m) => m [Text]
selectDescriptionQuery = select descriptionQuery

countProfiles :: Select (Expr Int, Expr Int64, Expr Text, Expr Bool)
countProfiles =
  aggregate $ do
    p <- from profile
    name_ <- groupBy $ p ^. #name
    pure
      ( count $ p ^. #name
      , sum_ $ p ^. #age
      , name_
      , all_ $ p ^. #boolean
      )

lol :: Select (Expr Text, Expr (Maybe Text))
lol = do
  p <- from profile
  p' <- leftJoin (from profile) $ \p' -> p' ^. #name ==. p ^. #name
  n <- view #name <$> from profile
  nom <- leftJoin (view #name <$> from profile) $ \n -> n ==. p ^. #name
  let name1 :: Expr Text
      name1 = p ^. #name
      name2 :: Expr (Maybe Text)
      name2 = p' ^. #name
      opt :: Expr (Maybe Text)
      opt = p' ^. #optional
  pure (name1, name2)

selectLol :: (MonadWoobat m) => m [(Text, Maybe Text)]
selectLol = select lol

profiles :: Select (HKD Profile Expr)
profiles = from profile

selectProfiles :: (MonadWoobat m) => m [Profile]
selectProfiles = select profiles

leftProfiles :: Select (HKD Profile (NullableF Expr))
leftProfiles = leftJoin (from profile) $ const true

selectLeftProfiles :: (MonadWoobat m) => m [Maybe Profile]
selectLeftProfiles = select leftProfiles

unit :: Select ()
unit = pure ()

selectUnit :: (MonadWoobat m) => m [()]
selectUnit = select unit

eqProfiles :: Select (Expr Bool, Expr Bool)
eqProfiles = do
  p1 <- from profile
  p2 <- from profile
  orderBy (p1 ^. #name) ascending
  pure (p1 ==. p2, p1 /=. p2)

selectEqProfiles :: (MonadWoobat m) => m [(Bool, Bool)]
selectEqProfiles = select eqProfiles

data HKDResult f = HKDResult
  { profile1 :: HKD Profile f
  , profile2 :: HKD Profile f
  }
  deriving (Generic, Barbies.FunctorB, Barbies.TraversableB, Barbies.ConstraintsB)

hkdQuery :: Select (HKDResult Expr)
hkdQuery = HKDResult <$> profiles <*> profiles

selectHKDQuery :: MonadWoobat m => m [HKDResult Identity]
selectHKDQuery = select hkdQuery
