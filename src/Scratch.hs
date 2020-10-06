{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans -Wno-simplifiable-class-constraints #-}

module Scratch where

import Control.Lens hiding (from)
import Data.Generic.HKD (HKD)
import qualified Data.Generic.HKD as HKD
import Data.Generics.Labels ()
import Data.Generics.Product.Fields (HasField')
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

data Profile = Profile
  { name :: !Text
  , description :: !Text
  , age :: !Int
  , optional :: !(Maybe Text)
  , boolean :: !Bool
  }
  deriving (Generic)

profile :: Table Profile
profile = table "profile"

ppp :: Profile
ppp =
  Profile
    { name = "Olle"
    , age = 33
    , description = ""
    , optional = Nothing
    , boolean = False
    }

descriptionQuery :: Select s (Expr s Text)
descriptionQuery = do
  p <- from profile
  where_ $ row p ==. value ppp
  where_ $ p ^. #name ==. "Olle"
  pure $ p ^. #description

selectDescriptionQuery :: (MonadWoobat m) => m [Text]
selectDescriptionQuery = select descriptionQuery

countProfiles :: Select s (Expr s Int, Expr s (Maybe Int), Expr s Text, Expr s Bool)
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

lol :: forall s. Select s (Expr s Text, Expr s (Maybe Text))
lol = do
  p <- from profile
  p' <- leftJoin (from profile) $ \p' -> p' ^. #name ==. p ^. #name
  n <- view #name <$> from profile
  nom <- leftJoin (view #name <$> from profile) $ \n -> n ==. p ^. #name
  let name1 :: Expr s Text
      name1 = p ^. #name
      name2 :: Expr s (Maybe Text)
      name2 = p' ^. #name
      opt :: Expr s (Maybe Text)
      opt = p' ^. #optional
  pure (name1, name2)

selectLol :: (MonadWoobat m) => m [(Text, Maybe Text)]
selectLol = select lol

profiles :: Select s (HKD Profile (Expr s))
profiles = from profile

selectProfiles :: (MonadWoobat m) => m [Profile]
selectProfiles = select profiles

leftProfiles :: Select s (HKD Profile (NullableF (Expr s)))
leftProfiles = leftJoin (from profile) $ const true

selectLeftProfiles :: (MonadWoobat m) => m [Maybe Profile]
selectLeftProfiles = select leftProfiles

unit :: Select s ()
unit = pure ()

selectUnit :: (MonadWoobat m) => m [()]
selectUnit = select unit

eqProfiles :: Select s (Expr s Bool, Expr s Bool)
eqProfiles = do
  p1 <- from profile
  p2 <- from profile
  orderBy (p1 ^. #name) ascending
  pure (p1 ==. p2, p1 /=. p2)

selectEqProfiles :: (MonadWoobat m) => m [(Bool, Bool)]
selectEqProfiles = select eqProfiles
