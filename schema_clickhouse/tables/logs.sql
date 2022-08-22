CREATE TABLE STG_ETH.LOGS
(
    log_index         UInt16,
    transaction_hash  FixedString(70),
    transaction_index UInt16,
    block_hash        FixedString(70),
    block_number      UInt64,
    address           FixedString(100),
    data              String,
    topics            String
)
    ENGINE = MergeTree()
        ORDER BY block_number;