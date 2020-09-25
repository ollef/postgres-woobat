{-# language AllowAmbiguousTypes #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language OverloadedStrings #-}
module Database.Woobat.Raw where

import ByteString.StrictBuilder (Builder)
import qualified ByteString.StrictBuilder as Builder
import Data.ByteString (ByteString)
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Data.String
import Data.Text (Text)
import qualified PostgreSQL.Binary.Encoding as Encoding

data SQL = SQL (Seq (Builder, Param)) Builder
  deriving Show

newtype Param = Param ByteString
  deriving (Show, IsString, Semigroup, Monoid)

instance IsString SQL where
  fromString = SQL mempty . fromString

instance Semigroup SQL where
  SQL codes1 code1 <> SQL Seq.Empty code2 =
    SQL codes1 (code1 <> code2)

  SQL codes1 code1 <> SQL ((code2, lit3) Seq.:<| codes4) code5 =
    SQL ((codes1 Seq.:|> (code1 <> code2, lit3)) <> codes4) code5

instance Monoid SQL where
  mempty = SQL mempty mempty

param :: ByteString -> SQL
param p = SQL (pure (mempty, Param p)) mempty

-------------------------------------------------------------------------------

class DatabaseType a where
  value :: a -> SQL
  typeName :: SQL

instance DatabaseType Text where
  value = param . Builder.builderBytes . Encoding.text_strict
  typeName = "text"
