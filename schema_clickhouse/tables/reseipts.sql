CREATE TABLE STG_ETH.RECEIPTS
(
    transaction_hash    FixedString(70),
    transaction_index   UInt16,
    block_hash          FixedString(70),
    block_number        UInt64,
    cumulative_gas_used UInt64,
    gas_used            UInt64,
    contract_address    FixedString(70),
    root                String,
    status              UInt8,
    effective_gas_price UInt64
)
    ENGINE = MergeTree()
        ORDER BY block_number;