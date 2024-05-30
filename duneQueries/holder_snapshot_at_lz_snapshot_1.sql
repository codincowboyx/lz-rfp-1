-- find the query here: https://dune.com/queries/3776279/6349827/

WITH all_chains_transfers AS (
  SELECT
    'ethereum' AS chain,
    to,
    tokenId,
    evt_block_time
  FROM erc721_ethereum.evt_Transfer
  WHERE
    contract_address IN (
        0xd9b78a2f1dafc8bb9c60961790d2beefebee56f4, 
        0x89e83f99bc48b9229ea7f2b9509a995e89c8472f, 
        0x5a1190759c9e7cf42da401639016f8f60affd465, 
        0xc1dcc70e27b187457709e0c72db3df941034ec6f)
    AND evt_block_time < TRY_CAST('2024-05-01 23:59:59' AS TIMESTAMP)
  UNION ALL
  SELECT
    'bnb' AS chain,
    to,
    tokenId,
    evt_block_time
  FROM erc721_bnb.evt_Transfer
  WHERE
    contract_address IN (
        0xefb872050656d1f3efeb4643df71b716bbf812d5,
        0xD8E0f750e34C4bf8bbEdD09e35BaFc8A78483584,
        0xfeafdc67892a8d00869fd081b8307ef18eaaa62b,
        0xa784491d2a8361051329f1a301f7390cc51af6c6)
    AND evt_block_time < TRY_CAST('2024-05-01 23:59:59' AS TIMESTAMP)
  UNION ALL
  SELECT
    'polygon' AS chain,
    to,
    tokenId,
    evt_block_time
  FROM erc721_polygon.evt_transfer
  WHERE
    contract_address IN (
        0xaaeef52ad4695b8e3b758215ca6bbca4d7680c62,
        0xd9026bfcdd38a260567519cf7db97616489d896b,
        0xfeafdc67892a8d00869fd081b8307ef18eaaa62b,
        0x5cab0996dc2d1b5933da89553907ba010a80d97e)
    AND evt_block_time < TRY_CAST('2024-05-01 23:59:59' AS TIMESTAMP)
  UNION ALL
  SELECT
    'arbitrum' AS chain,
    to,
    tokenId,
    evt_block_time
  FROM erc721_arbitrum.evt_transfer
  WHERE
    contract_address IN (
        0xaaeef52ad4695b8e3b758215ca6bbca4d7680c62,
        0xbcd40bd88be04996903eedd76d221fd68d796969,
        0x907d2a4e0dcd20d614850800ecf83b4f59b708df,
        0xaec825b1c6e5154f07884e2aa35eb6247a14b009)
    AND evt_block_time < TRY_CAST('2024-05-01 23:59:59' AS TIMESTAMP)
  UNION ALL
  SELECT
    'optimism' AS chain,
    to,
    tokenId,
    evt_block_time
  FROM erc721_optimism.evt_transfer
  WHERE
    contract_address IN (
        0xaaeef52ad4695b8e3b758215ca6bbca4d7680c62,
        0xbcd40bd88be04996903eedd76d221fd68d796969,
        0x907d2a4e0dcd20d614850800ecf83b4f59b708df,
        0x8c648dd5d34d9b3b1b4184333e5c15843339f76b)
    AND evt_block_time < TRY_CAST('2024-05-01 23:59:59' AS TIMESTAMP)
  UNION ALL
  SELECT
    'fantom' AS chain,
    to,
    tokenId,
    evt_block_time
  FROM erc721_fantom.evt_transfer
  WHERE
    contract_address IN (
        0xaAEEf52Ad4695b8e3B758215ca6BBCa4D7680C62,
        0xd9026bfcdd38a260567519cf7db97616489d896b,
        0x907d2a4e0dcd20d614850800ecf83b4f59b708df,
        0x5cab0996dc2d1b5933da89553907ba010a80d97e)
    AND evt_block_time < TRY_CAST('2024-05-01 23:59:59' AS TIMESTAMP)
  UNION ALL
  SELECT
    'avalanche' AS chain,
    to,
    tokenId,
    evt_block_time
  FROM erc721_avalanche_c.evt_Transfer
  WHERE
    contract_address IN (
        0xaaeef52ad4695b8e3b758215ca6bbca4d7680c62,
        0x9fc531cf9b2fdb8f11f08391c965c2bad42a1f5c,
        0x907d2a4e0dcd20d614850800ecf83b4f59b708df,
        0x8c648dd5d34d9b3b1b4184333e5c15843339f76b)
    AND evt_block_time < TRY_CAST('2024-05-01 23:59:59' AS TIMESTAMP)
), last_owners AS (
  SELECT
    chain,
    to AS holder,
    tokenId,
    RANK() OVER (PARTITION BY tokenId ORDER BY evt_block_time DESC) AS rn
  FROM all_chains_transfers
)
SELECT
  holder,
  COUNT(DISTINCT tokenId) AS token_count
FROM last_owners
WHERE
  rn = 1
GROUP BY
  holder