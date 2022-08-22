CREATE TABLE STG_ETH.TOKEN_TRANSFERS
(
    token_address    FixedString(70),
    from_address     FixedString(70),
    to_address       FixedString(70),
    value            String,
    transaction_hash FixedString(70),
    log_index        UInt32,
    block_number     UInt32
)
    ENGINE = MergeTree()
        ORDER BY block_number;