CREATE TABLE STG_ETH.BLOCKS
(
    number            UInt32,
    hash              FixedString(70),
    parent_hash       FixedString(70),
    nonce             FixedString(30),
    sha3_uncles       FixedString(70),
    logs_bloom        String,
    transactions_root FixedString(70),
    state_root        FixedString(70),
    receipts_root     FixedString(70),
    miner             FixedString(60),
    difficulty        UInt256,
    total_difficulty  UInt256,
    size              UInt32,
    extra_data        String,
    gas_limit         UInt64,
    gas_used          UInt64,
    timestamp         UInt32,
    transaction_count UInt16,
    base_fee_per_gas  UInt64
)
ENGINE = MergeTree()
ORDER BY number;
--PARTITION BY number