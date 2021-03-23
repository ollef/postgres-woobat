{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ViewPatterns #-}

module Database.Woobat.Raw where

import ByteString.StrictBuilder (Builder)
import qualified ByteString.StrictBuilder as Builder
import Control.Exception.Safe
import Control.Monad
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import Data.Foldable
import Data.HashMap.Lazy (HashMap)
import Data.List (intersperse)
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq
import Data.String
import qualified Database.PostgreSQL.LibPQ as LibPQ
import qualified Database.Woobat.Monad as Monad
import qualified PostgreSQL.Binary.Encoding as Encoding

newtype SQL = SQL (Seq SQLFragment)
  deriving (Show)

data SQLFragment
  = Code !Builder
  | Param !ByteString
  | NullParam
  deriving (Show)

instance IsString SQLFragment where
  fromString = Code . fromString

instance IsString SQL where
  fromString = SQL . pure . fromString

instance Semigroup SQL where
  SQL (codes1 Seq.:|> Code code2) <> SQL (Code code3 Seq.:<| codes4) =
    SQL $ (codes1 Seq.:|> Code (code2 <> code3)) <> codes4
  SQL codes1 <> SQL codes2 = SQL $ codes1 <> codes2

instance Monoid SQL where
  mempty = SQL mempty

separateBy :: (Foldable f, Monoid a) => a -> f a -> a
separateBy separator =
  mconcat . intersperse separator . toList

code :: ByteString -> SQL
code = SQL . pure . Code . Builder.bytes

param :: ByteString -> SQL
param = SQL . pure . Param

nullParam :: SQL
nullParam = SQL $ pure NullParam

-------------------------------------------------------------------------------

newtype Expr = Expr {unExpr :: HashMap ByteString Int -> SQL}
  deriving (Semigroup, Monoid)

instance Show Expr where
  show (Expr f) = show $ f mempty

instance IsString Expr where
  fromString = Expr . const . fromString

codeExpr :: ByteString -> Expr
codeExpr = Expr . pure . code

paramExpr :: ByteString -> Expr
paramExpr = Expr . pure . param

nullParamExpr :: Expr
nullParamExpr = Expr $ pure nullParam

-------------------------------------------------------------------------------

data Select = Select
  { from :: !(From ())
  , wheres :: Tsil SQL
  , groupBys :: Tsil SQL
  , orderBys :: Tsil (SQL, Order)
  , limit :: !Limit
  }
  deriving (Show)

fromView :: Select -> Maybe (From ())
fromView Select {..} =
  case (wheres, groupBys, orderBys, limit) of
    (Empty, Empty, Empty, (== mempty) -> True) -> Just from
    _ -> Nothing

data Order = Ascending | Descending
  deriving (Eq, Show)

data From unitAlias
  = Unit !unitAlias
  | Table !ByteString !ByteString
  | Set !SQL !SQL
  | Subquery [(SQL, ByteString)] !Select !ByteString
  | CrossJoin !(From ByteString) !(From ByteString)
  | LeftJoin !(From ByteString) !SQL !(From ByteString)
  deriving (Functor, Foldable, Traversable, Show)

instance Semigroup Select where
  Select a1 b1 c1 d1 e1 <> Select a2 b2 c2 d2 e2 =
    Select (a1 <> a2) (b1 <> b2) (c1 <> c2) (d1 <> d2) (e1 <> e2)

instance Monoid Select where
  mempty = Select mempty mempty mempty mempty mempty

data Limit = Limit
  { count :: Maybe Int
  , offset :: !Int
  }
  deriving (Eq, Show)

instance Semigroup Limit where
  Limit c1 o1 <> Limit c2 o2 =
    Limit
      ( case (c1, c2) of
          (Nothing, Nothing) -> Nothing
          (Just _, Nothing) -> c1
          (Nothing, Just c2') -> Just $ max (c2' - o1) 0
          (Just c1', Just c2') -> Just $ min c1' (max (c2' - o1) 0)
      )
      (o1 + o2)

instance Monoid Limit where
  mempty =
    Limit
      { count = Nothing
      , offset = 0
      }

unitView :: From unitAlias -> Either (From unitAlias') unitAlias
unitView (Unit alias) = Right alias
unitView (Table table alias) = Left (Table table alias)
unitView (Set expr alias) = Left (Set expr alias)
unitView (Subquery results select alias) = Left (Subquery results select alias)
unitView (CrossJoin from1 from2) = Left (CrossJoin from1 from2)
unitView (LeftJoin from1 on from2) = Left (LeftJoin from1 on from2)

instance Semigroup (From unitAlias) where
  outerFrom1 <> outerFrom2 =
    case (unitView outerFrom1, unitView outerFrom2) of
      (Right _, _) -> outerFrom2
      (_, Right _) -> outerFrom1
      (Left from1, Left from2) -> CrossJoin from1 from2

instance Monoid (From ()) where
  mempty = Unit ()

-------------------------------------------------------------------------------

data OnConflict
  = NoConflictHandling
  | OnAnyConflictDoNothing
  | OnConflict [ByteString] [(ByteString, Expr)] (Maybe Expr)

-------------------------------------------------------------------------------

data Tsil a = Empty | Tsil a :> a
  deriving (Eq, Ord, Show, Functor, Traversable)

instance Foldable Tsil where
  foldMap _ Empty = mempty
  foldMap f (xs :> x) = foldMap f xs `mappend` f x

  toList = reverse . go
    where
      go Empty = []
      go (xs :> x) = x : go xs

instance Semigroup (Tsil a) where
  xs <> Empty = xs
  xs <> ys :> y = (xs <> ys) :> y

instance Monoid (Tsil a) where
  mempty = Empty

instance Applicative Tsil where
  pure = (Empty :>)
  (<*>) = ap

instance Monad Tsil where
  return = pure
  Empty >>= _ = Empty
  xs :> x >>= f = (xs >>= f) <> f x

-------------------------------------------------------------------------------

compileSelect :: [SQL] -> Select -> SQL
compileSelect exprs Select {from, wheres, groupBys, orderBys, limit} =
  "SELECT " <> separateBy ", " exprs
    <> ( case unitView from of
          Right () -> mempty
          Left from' -> " FROM " <> compileFrom from'
       )
    <> compileWheres wheres
    <> compileGroupBys groupBys
    <> compileOrderBys orderBys
    <> compileLimit limit

compileFrom :: From ByteString -> SQL
compileFrom from =
  case from of
    Unit alias ->
      "(VALUES (0)) " <> code alias
    Table name alias
      | name == alias ->
        code name
      | otherwise ->
        code name <> " AS " <> code alias
    Set expr returnRow ->
      expr <> " AS " <> returnRow
    Subquery exprAliases select alias ->
      "LATERAL ("
        <> compileSelect [expr <> " AS " <> code columnAlias | (expr, columnAlias) <- exprAliases] select
        <> ") AS "
        <> code alias
    CrossJoin left right ->
      compileFrom left <> " CROSS JOIN " <> compileFrom right
    LeftJoin left on right ->
      compileFrom left <> " LEFT JOIN " <> compileFrom right <> " ON " <> on

compileWheres :: Tsil SQL -> SQL
compileWheres Empty = mempty
compileWheres wheres = " WHERE " <> separateBy " AND " wheres

compileGroupBys :: Tsil SQL -> SQL
compileGroupBys Empty = mempty
compileGroupBys groupBys = " GROUP BY " <> separateBy ", " groupBys

compileOrderBys :: Tsil (SQL, Order) -> SQL
compileOrderBys Empty = mempty
compileOrderBys orderBys =
  " ORDER BY "
    <> separateBy ", " ((\(expr, order) -> expr <> " " <> compileOrder order) <$> orderBys)

compileOrder :: Order -> SQL
compileOrder Ascending = "ASC"
compileOrder Descending = "DESC"

compileLimit :: Limit -> SQL
compileLimit l =
  case count l of
    Nothing -> ""
    Just n -> " LIMIT " <> param (Builder.builderBytes $ Encoding.int8_int64 $ fromIntegral n) <> "::int8"
    <> case offset l of
      0 -> ""
      o -> " OFFSET " <> param (Builder.builderBytes $ Encoding.int8_int64 $ fromIntegral o) <> "::int8"

compileOnConflict :: OnConflict -> Expr
compileOnConflict onConflict =
  Expr $ \usedNames -> case onConflict of
    NoConflictHandling -> ""
    OnAnyConflictDoNothing -> " ON CONFLICT DO NOTHING"
    OnConflict fields assignments maybeWhere -> do
      " ON CONFLICT (" <> separateBy ", " (code <$> fields) <> ") "
        <> case assignments of
          [] -> "DO NOTHING"
          _ -> "DO UPDATE " <> separateBy ", " ["SET " <> code f <> " = " <> unExpr e usedNames | (f, e) <- assignments]
        <> case maybeWhere of
          Nothing -> ""
          Just e -> " WHERE " <> unExpr e usedNames

-------------------------------------------------------------------------------

execute :: Monad.MonadWoobat m => SQL -> (LibPQ.Result -> IO a) -> m a
execute sql onResult =
  Monad.withConnection $ \connection -> liftIO $ do
    let (code_, params) = separateCodeAndParams sql
        params' = fmap (LibPQ.Oid 0,,LibPQ.Binary) <$> params
    maybeResult <- LibPQ.execParams connection code_ params' LibPQ.Binary
    case maybeResult of
      Nothing -> throwM $ Monad.ConnectionError LibPQ.ConnectionBad
      Just result -> do
        status <- LibPQ.resultStatus result
        let onError = do
              message <- LibPQ.resultErrorMessage result
              throwM $ Monad.ExecutionError status message
        case status of
          LibPQ.EmptyQuery -> onError
          LibPQ.CommandOk -> onResult result
          LibPQ.CopyOut -> onError
          LibPQ.CopyIn -> onError
          LibPQ.CopyBoth -> onError
          LibPQ.BadResponse -> onError
          LibPQ.NonfatalError -> onError
          LibPQ.FatalError -> onError
          LibPQ.SingleTuple -> onResult result
          LibPQ.TuplesOk -> onResult result
  where
    separateCodeAndParams :: SQL -> (ByteString, [Maybe ByteString])
    separateCodeAndParams (SQL fragments) = do
      let (codeBuilder, params, _) = foldl' go (mempty, mempty, 1 :: Int) fragments
      (Builder.builderBytes codeBuilder, toList params)
      where
        go (!codeBuilder, params, !paramNumber) fragment =
          case fragment of
            Code c -> (codeBuilder <> c, params, paramNumber)
            Param p -> (codeBuilder <> "$" <> Builder.asciiIntegral paramNumber, params :> Just p, paramNumber + 1)
            NullParam -> (codeBuilder <> "$" <> Builder.asciiIntegral paramNumber, params :> Nothing, paramNumber + 1)
