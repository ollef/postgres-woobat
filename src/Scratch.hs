{-# language DataKinds #-}
{-# language DeriveGeneric #-}
{-# language DuplicateRecordFields #-}
{-# language OverloadedLabels #-}
{-# language OverloadedStrings #-}
{-# language ScopedTypeVariables #-}
{-# language FlexibleContexts #-}
{-# language TypeApplications #-}
{-# language TypeSynonymInstances #-}
{-# language FlexibleInstances #-}
{-# language MultiParamTypeClasses #-}
{-# options_ghc -fno-warn-orphans -Wno-simplifiable-class-constraints #-}
module Scratch where

import Data.Text (Text)
import Database.Woobat
import Control.Lens
import GHC.Generics
import Data.Generic.HKD
import Data.Generics.Product.Fields (HasField')
import Data.Generics.Labels ()
import GHC.OverloadedLabels (IsLabel(fromLabel))

-- | Turn #label syntax into lenses for HKD structures
instance
  {-# OVERLAPPING #-}
  ( Functor f
  , HasField' field (HKD structure g) (g inner)
  ) =>
  IsLabel field ((g inner -> f (g inner)) -> HKD structure g -> f (HKD structure g)) where
  fromLabel = field @field

data Profile = Profile
  { name :: !Text
  , description :: !Text
  } deriving Generic

profile :: Table Profile
profile = Table
  { name = "profile"
  , fieldNameModifier = id
  }

ppp = Profile { name = "Olle", description = "" } ^. #name

descriptionQuery :: Query s (Expr s Text)
descriptionQuery = do
  p <- select profile
  where_ $ p ^. #name ==. "Olle"
  pure $ p ^. #name


