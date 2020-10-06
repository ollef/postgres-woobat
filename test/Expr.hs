{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}

module Expr where

import Control.Exception.Safe
import Control.Monad.Reader
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Lazy as ByteString.Lazy
import qualified Data.Char as Char
import Data.Int
import qualified Data.List.NonEmpty as NonEmpty
import Data.Scientific
import qualified Data.Text.Lazy as Text.Lazy
import Data.Time
import Data.Typeable
import Database.Woobat
import GHC.Generics
import qualified Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

properties :: [(Hedgehog.PropertyName, Hedgehog.Property)]
properties =
  [
    ( "select nothing"
    , Hedgehog.withTests 1 $
        Hedgehog.property $ do
          result <- runPureWoobat $ select $ pure ()
          result Hedgehog.=== [()]
    )
  ,
    ( "roundtrip values through database"
    , Hedgehog.withTests 500 $
        Hedgehog.property $ do
          Some gen <- Hedgehog.forAll genSome
          x <- Hedgehog.forAll gen
          result <- Hedgehog.evalM $ runPureWoobat $ select $ pure $ value x
          result Hedgehog.=== [x]
    )
  ,
    ( "database equality is reflexive"
    , Hedgehog.property $ do
        Some gen <- Hedgehog.forAll genSome
        x <- Hedgehog.forAll gen
        result <- Hedgehog.evalM $ runPureWoobat $ select $ pure (value x ==. value x, value x /=. value x)
        result Hedgehog.=== [(x == x, x /= x)]
    )
  ,
    ( "database equality matches Haskell equality"
    , Hedgehog.property $ do
        Some gen <- Hedgehog.forAll genSome
        x <- Hedgehog.forAll gen
        y <- Hedgehog.forAll gen
        result <- Hedgehog.evalM $ runPureWoobat $ select $ pure (value x ==. value y, value x /=. value y)
        result Hedgehog.=== [(x == y, x /= y)]
    )
  ,
    ( "database Num matches Haskell Num"
    , Hedgehog.property $ do
        SomeIntegral gen <- Hedgehog.forAll genSomeIntegral
        x <- Hedgehog.forAll gen
        y <- Hedgehog.forAll gen
        when (overflows (+) x y || overflows (-) x y || overflows (*) x 5) Hedgehog.discard
        result <- Hedgehog.evalM $ runPureWoobat $ select $ pure (value x + value y, value x - value y, value x * 5, abs (value x), signum (value x))
        result Hedgehog.=== [(x + y, x - y, x * 5, abs x, signum x)]
    )
  ,
    ( "`mod_ x` matches `(if x < 0 then negate else id) $ mod (abs x) (abs y)`"
    , Hedgehog.property $ do
        SomeIntegral gen <- Hedgehog.forAll genSomeIntegral
        x <- Hedgehog.forAll gen
        y <- Hedgehog.forAll $ Gen.filter (/= 0) gen
        result <- Hedgehog.evalM $ runPureWoobat $ select $ pure (mod_ (value x) (value y))
        result Hedgehog.=== [(if x < 0 then negate else id) $ mod (abs x) (abs y)]
    )
  ,
    ( "if_"
    , Hedgehog.property $ do
        Some gen <- Hedgehog.forAll genSome
        cond1 <- Hedgehog.forAll Gen.bool
        cond2 <- Hedgehog.forAll Gen.bool
        br1 <- Hedgehog.forAll gen
        br2 <- Hedgehog.forAll gen
        def <- Hedgehog.forAll gen
        result <- Hedgehog.evalM $ runPureWoobat $ select $ pure (if_ [(value cond1, value br1), (value cond2, value br2)] $ value def)
        result Hedgehog.=== [if cond1 then br1 else if cond2 then br2 else def]
    )
  ,
    ( "not_"
    , Hedgehog.withTests 1 $
        Hedgehog.property $ do
          result <- Hedgehog.evalM $ runPureWoobat $ select $ pure (not_ true, not_ false)
          result Hedgehog.=== [(False, True)]
    )
  ,
    ( "&&. and ||."
    , Hedgehog.withTests 1 $
        Hedgehog.property $ do
          result <-
            Hedgehog.evalM $
              runPureWoobat $
                select $
                  pure $
                    array $ concat [[b1 &&. b2, b1 ||. b2] | b1 <- [false, true], b2 <- [false, true]]

          result Hedgehog.=== [concat [[b1 && b2, b1 || b2] | b1 <- [False, True], b2 <- [False, True]]]
    )
  ,
    ( "Ordering matches Haskell ordering"
    , Hedgehog.property $ do
        SomeNum gen <- Hedgehog.forAll genSomeNum
        x <- Hedgehog.forAll gen
        y <- Hedgehog.forAll gen
        result <-
          Hedgehog.evalM $
            runPureWoobat $
              select $ pure (value x <. value y, value x <=. value y, value x >. value y, value x >=. value y)

        result Hedgehog.=== [(x < y, x <= y, x > y, x >= y)]
    )
  ,
    ( "maximum_ and minimum_"
    , Hedgehog.property $ do
        SomeNum gen <- Hedgehog.forAll genSomeNum
        x <- Hedgehog.forAll gen
        xs <- Hedgehog.forAll $ Gen.list (Range.linearFrom 0 0 10) gen
        result <-
          Hedgehog.evalM $
            runPureWoobat $
              select $ pure (maximum_ (value <$> x NonEmpty.:| xs), minimum_ (value <$> x NonEmpty.:| xs))

        result Hedgehog.=== [(maximum (x : xs), minimum (x : xs))]
    )
  ]

runPureWoobat :: MonadIO m => Woobat a -> m a
runPureWoobat =
  liftIO
    . runWoobatT
      ( ByteString.intercalate
          " "
          [ "host=localhost"
          , "port=5432"
          , "user=woobat"
          , "dbname=woobat"
          , "client_encoding=UTF8"
          ]
      )

-------------------------------------------------------------------------------

data Some where
  Some :: (Typeable a, Show a, Ord a, FromJSON a) => Hedgehog.Gen a -> Some

data SomeFractional where
  SomeFractional ::
    ( Typeable a
    , Show a
    , Ord a
    , Fractional a
    , forall s. Fractional (Expr s a)
    , FromJSON a
    , NonNestedMaybe a
    , NonNestedArray a
    ) =>
    Hedgehog.Gen a ->
    SomeFractional

data SomeNum where
  SomeNum ::
    ( Typeable a
    , Show a
    , Ord a
    , Num a
    , FromJSON a
    , NonNestedMaybe a
    , NonNestedArray a
    ) =>
    Hedgehog.Gen a ->
    SomeNum

data SomeIntegral where
  SomeIntegral ::
    ( Typeable a
    , Show a
    , Integral a
    , FromJSON a
    , NonNestedMaybe a
    , NonNestedArray a
    ) =>
    Hedgehog.Gen a ->
    SomeIntegral

data SomeNonNested where
  SomeNonNested ::
    ( Typeable a
    , Show a
    , Ord a
    , FromJSON a
    , NonNestedMaybe a
    , NonNestedArray a
    ) =>
    Hedgehog.Gen a ->
    SomeNonNested

data SomeNonArray where
  SomeNonArray ::
    ( Typeable a
    , Show a
    , Ord a
    , FromJSON a
    , NonNestedArray a
    ) =>
    Hedgehog.Gen a ->
    SomeNonArray

data SomeNonMaybe where
  SomeNonMaybe ::
    ( Typeable a
    , Show a
    , Ord a
    , FromJSON a
    , NonNestedMaybe a
    ) =>
    Hedgehog.Gen a ->
    SomeNonMaybe

instance Show Some where
  show (Some gena) = show (typeOf (unGen gena))
    where
      unGen :: Hedgehog.Gen a -> a
      unGen = undefined

instance Show SomeNum where
  show (SomeNum gena) = show (typeOf (unGen gena))
    where
      unGen :: Hedgehog.Gen a -> a
      unGen = undefined

instance Show SomeIntegral where
  show (SomeIntegral gena) = show (typeOf (unGen gena))
    where
      unGen :: Hedgehog.Gen a -> a
      unGen = undefined

instance Show SomeFractional where
  show (SomeFractional gena) = show (typeOf (unGen gena))
    where
      unGen :: Hedgehog.Gen a -> a
      unGen = undefined

data TableTwo a b = TableTwo
  { field1 :: a
  , field2 :: b
  }
  deriving (Eq, Ord, Show, Generic)

genSome :: Hedgehog.MonadGen m => m Some
genSome =
  Gen.choice
    [ do
        SomeNonNested x <- genSomeNonNested
        pure $ Some x
    , do
        SomeNonMaybe x <- genSomeNonMaybe
        pure $ Some $ Gen.maybe x
    , do
        SomeNonMaybe x <- genSomeArray
        pure $ Some x
    ]

genSomeArray :: Hedgehog.MonadGen m => m SomeNonMaybe
genSomeArray = do
  SomeNonArray x <- genSomeNonArray
  pure $ SomeNonMaybe $ Gen.list (Range.linearFrom 0 0 10) x

genSomeNonMaybe :: Hedgehog.MonadGen m => m SomeNonMaybe
genSomeNonMaybe =
  Gen.choice
    [ do
        SomeNonNested x <- genSomeNonNested
        pure $ SomeNonMaybe x
    , genSomeArray
    ]

genSomeNonArray :: Hedgehog.MonadGen m => m SomeNonArray
genSomeNonArray = do
  SomeNonNested x <- genSomeNonNested
  Gen.element [SomeNonArray x, SomeNonArray $ Gen.maybe x]

genSomeNonNested :: Hedgehog.MonadGen m => m SomeNonNested
genSomeNonNested =
  Gen.recursive
    Gen.choice
    ( [ pure $ SomeNonNested Gen.bool
      , pure $ SomeNonNested unicode
      , pure $ SomeNonNested $ Gen.text (Range.linearFrom 0 0 10000) unicode
      , pure $ SomeNonNested $ Text.Lazy.fromStrict <$> Gen.text (Range.linearFrom 0 0 10000) unicode
      , pure $ SomeNonNested $ Gen.bytes (Range.linearFrom 0 0 10000)
      , pure $ SomeNonNested $ ByteString.Lazy.fromStrict <$> Gen.bytes (Range.linearFrom 0 0 10000)
      , pure $ SomeNonNested genDay
      , pure $ SomeNonNested genTimeOfDay
      , pure $ SomeNonNested $ (,) <$> genTimeOfDay <*> genTimeZone
      , pure $ SomeNonNested genLocalTime
      , pure $ SomeNonNested genUTCTime
      , pure $ SomeNonNested genDiffTime
      , do
          SomeNum x <- genSomeNum
          pure $ SomeNonNested x
      ]
    )
    [ do
        Some gena <- genSome
        Some genb <- genSome
        pure $
          SomeNonNested $ do
            a <- gena
            b <- genb
            pure $ TableTwo {field1 = a, field2 = b}
    ]

genSomeFractional :: Hedgehog.MonadGen m => m SomeFractional
genSomeFractional =
  Gen.element
    [ SomeFractional $ Gen.float (Range.exponentialFloatFrom 0 (-1000000000) 1000000000)
    , SomeFractional $ Gen.double (Range.exponentialFloatFrom 0 (-1000000000) 1000000000)
    ]

genSomeNum :: Hedgehog.MonadGen m => m SomeNum
genSomeNum =
  Gen.choice
    [ do
        SomeFractional x <- genSomeFractional
        pure $ SomeNum x
    , do
        SomeIntegral x <- genSomeIntegral
        pure $ SomeNum x
    , pure $ SomeNum $ (scientific <$> Gen.integral (Range.linearFrom 0 (-1000000000000000000) 1000000000000000000000000) <*> Gen.integral (Range.linearFrom 0 (-1000) 1000))
    ]

genSomeIntegral :: Hedgehog.MonadGen m => m SomeIntegral
genSomeIntegral =
  Gen.element
    [ SomeIntegral $ Gen.int (fromIntegral <$> Range.linearBounded @Int16)
    , SomeIntegral $ Gen.int16 Range.linearBounded
    , SomeIntegral $ Gen.int32 Range.linearBounded
    , SomeIntegral $ Gen.int64 Range.linearBounded
    ]

overflows :: Integral b => (forall a. Num a => a -> a -> a) -> b -> b -> Bool
overflows op x y = fromIntegral (op x y) /= op (fromIntegral x :: Integer) (fromIntegral y)

-------------------------------------------------------------------------------

genDay :: Hedgehog.MonadGen m => m Day
genDay = do
  y <- Gen.integral (Range.constant 2000 2019)
  m <- Gen.int (Range.constant 1 12)
  d <- Gen.int (Range.constant 1 28)
  pure $ fromGregorian y m d

genTimeOfDay :: Hedgehog.MonadGen m => m TimeOfDay
genTimeOfDay = do
  h <- Gen.integral $ Range.constant 0 23
  m <- Gen.integral $ Range.constant 0 59
  s <- Gen.integral $ Range.constant 0 59
  pure $ TimeOfDay h m $ fromInteger s

genTimeZone :: Hedgehog.MonadGen m => m TimeZone
genTimeZone = do
  m <- Gen.integral $ Range.constant ((-12) * 60) (14 * 60)
  pure $ TimeZone m False ""

genLocalTime :: Hedgehog.MonadGen m => m LocalTime
genLocalTime = LocalTime <$> genDay <*> genTimeOfDay

genUTCTime :: Hedgehog.MonadGen m => m UTCTime
genUTCTime = do
  day <- genDay
  secs <- Gen.integral (Range.constant 0 86401)
  let diff = secondsToDiffTime secs
  pure $ UTCTime day diff

genDiffTime :: Hedgehog.MonadGen m => m DiffTime
genDiffTime = do
  let maxDiffTime = 1000000 * yearSeconds
      minDiffTime = - maxDiffTime
  secondsToDiffTime . fromIntegral <$> Gen.integral (Range.linearFrom 0 minDiffTime maxDiffTime)

yearSeconds, daySeconds, hourSeconds, minuteSeconds :: Int64
yearSeconds = truncate (365.2425 * fromIntegral daySeconds :: Rational)
daySeconds = 24 * hourSeconds
hourSeconds = 60 * minuteSeconds
minuteSeconds = 60

-------------------------------------------------------------------------------

unicode :: Hedgehog.Gen Char
unicode = do
  c <- Gen.unicode
  if Char.ord c == 0
    then Gen.discard
    else pure c
