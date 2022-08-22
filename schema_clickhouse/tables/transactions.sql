CREATE TABLE STG_ETH.TRANSACTIONS
(
    hash                     FixedString(70),
    nonce                    FixedString(30),
    block_hash               FixedString(70),
    block_number             UInt32,
    transaction_index        UInt16,
    from_address             FixedString(70),
    to_address               FixedString(70),
    VALUE                    UInt256,
    gas                      UInt64,
    gas_price                UInt64,
    INPUT                    String,
    block_timestamp          DateTime,
    max_fee_per_gas          UInt64,
    max_priority_fee_per_gas UInt64,
    transaction_type         UInt8
) ENGINE = MergeTree()
ORDER BY block_number;