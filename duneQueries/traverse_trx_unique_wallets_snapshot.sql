-- find the query here: https://dune.com/queries/3776350/6349914/

WITH contract_calls AS (
  SELECT
    "from" AS wallet
  FROM ethereum.transactions
  WHERE
    "to" IN (
        0xd9b78a2f1dafc8bb9c60961790d2beefebee56f4, 
        0x89e83f99bc48b9229ea7f2b9509a995e89c8472f, 
        0xc1dcc70e27b187457709e0c72db3df941034ec6f, 
        0x5a1190759c9e7cf42da401639016f8f60affd465)
    AND SUBSTRING(TRY_CAST("data" AS VARCHAR), 1, 10) = '0xcf89fa03'
    AND block_time < TRY_CAST('2024-05-01 23:59:59' AS TIMESTAMP)
  UNION ALL
  SELECT
    "from" AS wallet
  FROM bnb.transactions
  WHERE
    "to" IN (
        0xefb872050656d1f3efeb4643df71b716bbf812d5, 
        0xfeafdc67892a8d00869fd081b8307ef18eaaa62b, 
        0xa784491d2a8361051329f1a301f7390cc51af6c6, 
        0xD8E0f750e34C4bf8bbEdD09e35BaFc8A78483584)
    AND SUBSTRING(TRY_CAST("data" AS VARCHAR), 1, 10) = '0xcf89fa03'
    AND block_time < TRY_CAST('2024-05-01 23:59:59' AS TIMESTAMP)
  UNION ALL
  SELECT
    "from" AS wallet
  FROM avalanche_c.transactions
  WHERE
    "to" IN (
        0xaaeef52ad4695b8e3b758215ca6bbca4d7680c62, 
        0x9fc531cf9b2fdb8f11f08391c965c2bad42a1f5c, 
        0x8c648dd5d34d9b3b1b4184333e5c15843339f76b, 
        0x907d2a4e0dcd20d614850800ecf83b4f59b708df)
    AND SUBSTRING(TRY_CAST("data" AS VARCHAR), 1, 10) = '0xcf89fa03'
    AND block_time < TRY_CAST('2024-05-01 23:59:59' AS TIMESTAMP)
  UNION ALL
  SELECT
    "from" AS wallet
  FROM polygon.transactions
  WHERE
    "to" IN (
        0xaaeef52ad4695b8e3b758215ca6bbca4d7680c62, 
        0xd9026bfcdd38a260567519cf7db97616489d896b, 
        0x5cab0996dc2d1b5933da89553907ba010a80d97e, 
        0xfeafdc67892a8d00869fd081b8307ef18eaaa62b)
    AND SUBSTRING(TRY_CAST("data" AS VARCHAR), 1, 10) = '0xcf89fa03'
    AND block_time < TRY_CAST('2024-05-01 23:59:59' AS TIMESTAMP)
UNION ALL
  SELECT
    "from" AS wallet
  FROM arbitrum.transactions
  WHERE
    "to" IN (
        0xaaeef52ad4695b8e3b758215ca6bbca4d7680c62, 
        0xbcd40bd88be04996903eedd76d221fd68d796969, 
        0xaec825b1c6e5154f07884e2aa35eb6247a14b009, 
        0x907d2a4e0dcd20d614850800ecf83b4f59b708df)
    AND SUBSTRING(TRY_CAST("data" AS VARCHAR), 1, 10) = '0xcf89fa03'
    AND block_time < TRY_CAST('2024-05-01 23:59:59' AS TIMESTAMP)
    UNION ALL
  SELECT
    "from" AS wallet
  FROM optimism.transactions
  WHERE
    "to" IN (
        0xaaeef52ad4695b8e3b758215ca6bbca4d7680c62, 
        0xbcd40bd88be04996903eedd76d221fd68d796969, 
        0x8c648dd5d34d9b3b1b4184333e5c15843339f76b, 
        0x907d2a4e0dcd20d614850800ecf83b4f59b708df)
    AND SUBSTRING(TRY_CAST("data" AS VARCHAR), 1, 10) = '0xcf89fa03'
    AND block_time < TRY_CAST('2024-05-01 23:59:59' AS TIMESTAMP)
     UNION ALL
  SELECT
    "from" AS wallet
  FROM fantom.transactions
  WHERE
    "to" IN (
        0xaAEEf52Ad4695b8e3B758215ca6BBCa4D7680C62, 
        0xd9026bfcdd38a260567519cf7db97616489d896b, 
        0x5cab0996dc2d1b5933da89553907ba010a80d97e,
        0x907d2a4e0dcd20d614850800ecf83b4f59b708df)
    AND SUBSTRING(TRY_CAST("data" AS VARCHAR), 1, 10) = '0xcf89fa03'
    AND block_time < TRY_CAST('2024-05-01 23:59:59' AS TIMESTAMP)
)
SELECT
  DISTINCT wallet
FROM contract_calls