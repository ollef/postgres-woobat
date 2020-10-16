{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module Database.Woobat.Select (
  module Database.Woobat.Select,
  Database.Woobat.Barbie.Barbie,
  Database.Woobat.Select.Builder.Select,
) where

import Control.Exception.Safe
import Control.Monad
import Control.Monad.State
import qualified Data.Barbie as Barbie
import Data.Functor.Identity
import qualified Data.Generic.HKD as HKD
import qualified Database.PostgreSQL.LibPQ as LibPQ
import Database.Woobat.Barbie hiding (result)
import qualified Database.Woobat.Barbie
import qualified Database.Woobat.Compiler as Compiler
import Database.Woobat.Expr
import qualified Database.Woobat.Monad as Monad
import qualified Database.Woobat.Raw as Raw
import Database.Woobat.Scope
import Database.Woobat.Select.Builder
import qualified PostgreSQL.Binary.Decoding as Decoding

select ::
  forall a m.
  ( Monad.MonadWoobat m
  , Barbie (Expr ()) a
  , HKD.AllB DatabaseType (ToBarbie (Expr ()) a)
  , HKD.ConstraintsB (ToBarbie (Expr ()) a)
  , Resultable (FromBarbie (Expr ()) a Identity)
  ) =>
  Select () a ->
  m [Result (FromBarbie (Expr ()) a Identity)]
select s =
  Monad.withConnection $ \connection -> liftIO $ do
    let (rawSQL, resultsBarbie) = compile s
        (code, params) = Raw.separateCodeAndParams rawSQL
        params' = fmap (\p -> (LibPQ.Oid 0, p, LibPQ.Binary)) <$> params
    maybeResult <- LibPQ.execParams connection code params' LibPQ.Binary
    case maybeResult of
      Nothing -> throwM $ Monad.ConnectionError LibPQ.ConnectionBad
      Just result -> do
        status <- LibPQ.resultStatus result
        let onError = do
              message <- LibPQ.resultErrorMessage result
              throwM $ Monad.ExecutionError status message
            onResult = do
              rowCount <- LibPQ.ntuples result
              forM [0 .. rowCount - 1] $ \rowNumber -> do
                let go :: DatabaseType x => Expr () x -> StateT LibPQ.Column IO (Identity x)
                    go _ = fmap Identity $ do
                      col <- get
                      put $ col + 1
                      maybeValue <- liftIO $ LibPQ.getvalue result rowNumber col
                      case (decoder, maybeValue) of
                        (Decoder d, Just v) ->
                          case Decoding.valueParser d v of
                            Left err ->
                              throwM $ Monad.DecodingError rowNumber col err
                            Right a ->
                              pure a
                        (Decoder _, Nothing) ->
                          throwM $ Monad.UnexpectedNullError rowNumber col
                        (NullableDecoder _, Nothing) ->
                          pure Nothing
                        (NullableDecoder d, Just v) ->
                          case Decoding.valueParser d v of
                            Left err ->
                              throwM $ Monad.DecodingError rowNumber col err
                            Right a ->
                              pure $ Just a

                barbieRow :: ToBarbie (Expr ()) a Identity <-
                  flip evalStateT 0 $ Barbie.btraverseC @DatabaseType go resultsBarbie
                pure $ Database.Woobat.Barbie.result $ fromBarbie @(Expr ()) @a barbieRow
        case status of
          LibPQ.EmptyQuery -> onError
          LibPQ.CommandOk -> onError
          LibPQ.CopyOut -> onError
          LibPQ.CopyIn -> onError
          LibPQ.CopyBoth -> onError
          LibPQ.BadResponse -> onError
          LibPQ.NonfatalError -> onError
          LibPQ.FatalError -> onError
          LibPQ.SingleTuple -> onResult
          LibPQ.TuplesOk -> onResult

compile :: forall a. Barbie (Expr ()) a => Select () a -> (Raw.SQL, ToBarbie (Expr ()) a (Expr ()))
compile s = do
  let (results, st) = run mempty s
      resultsBarbie :: ToBarbie (Expr ()) a (Expr ())
      resultsBarbie = toBarbie results
      sql = Compiler.compileSelect (Barbie.bfoldMap (\(Expr e) -> [Raw.unExpr e $ usedNames st]) resultsBarbie) $ rawSelect st
  (sql, resultsBarbie)

orderBy :: Same s t => Expr s a -> Raw.Order -> Select t ()
orderBy (Expr expr) order_ =
  Select $ do
    usedNames_ <- gets usedNames
    addSelect mempty {Raw.orderBys = pure (Raw.unExpr expr usedNames_, order_)}

ascending :: Raw.Order
ascending = Raw.Ascending

descending :: Raw.Order
descending = Raw.Descending
