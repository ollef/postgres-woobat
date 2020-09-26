{-# language DataKinds #-}
{-# language DeriveGeneric #-}
{-# language DuplicateRecordFields #-}
{-# language FlexibleContexts #-}
{-# language FlexibleInstances #-}
{-# language MultiParamTypeClasses #-}
{-# language OverloadedLabels #-}
{-# language OverloadedStrings #-}
{-# language ScopedTypeVariables #-}
{-# language TypeApplications #-}
{-# language TypeFamilies #-}
{-# language TypeSynonymInstances #-}
{-# language UndecidableInstances #-}
{-# options_ghc -fno-warn-orphans -Wno-simplifiable-class-constraints #-}
module Scratch where

import Control.Lens hiding (from)
import Data.Generic.HKD (HKD)
import qualified Data.Generic.HKD as HKD
import Data.Generics.Labels ()
import Data.Generics.Product.Fields (HasField')
import Data.Text (Text)
import Database.Woobat
import GHC.Generics hiding (from)
import GHC.OverloadedLabels (IsLabel(fromLabel))

instance
  {-# OVERLAPPING #-}
  ( Functor f
  , HasField' field (HKD structure (NullableExpr s)) (NullableExpr s a)
  , na ~ Nullable a
  ) =>
  IsLabel field ((Expr s na -> f (Expr s na)) -> HKD structure (NullableExpr s) -> f (HKD structure (NullableExpr s))) where
  fromLabel = HKD.field @field . lens (\(NullableExpr e) -> e) const

data Profile = Profile
  { name :: !Text
  , description :: !Text
  , optional :: !(Maybe Text)
  } deriving Generic

profile :: Table Profile
profile = table "profile"

ppp = Profile { name = "Olle", description = "", optional = Nothing } ^. #name

descriptionQuery :: Select s (Expr s Text)
descriptionQuery = do
  p <- from profile
  where_ $ p ^. #name ==. "Olle"
  pure $ p ^. #name

countProfiles :: Select s (Expr s Text)
countProfiles =
  aggregate $ do
    p <- from profile
    name_ <- groupBy $ p ^. #name
    pure name_

lol :: forall s. Select s (Expr s Text, Expr s (Maybe Text))
lol = do
  p <- from profile
  p' <- leftJoin (from profile) $ \p' -> p' ^. #name ==. p ^. #name
  n <- view #name <$> from profile
  nom <- leftJoin (view #name <$> from profile) $ \n -> n ==. p ^. #name
  let
    name1 :: Expr s Text
    name1 = p ^. #name
    name2 :: Expr s (Maybe Text)
    name2 = p' ^. #name
    opt :: Expr s (Maybe Text)
    opt = p' ^. #optional
  pure (name1, name2)

profiles :: Select s (HKD Profile (Expr s))
profiles = from profile
