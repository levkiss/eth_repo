CREATE TABLE STG_ETH.CONTRACTS
(
    address     FixedString(70),
    description String,
    is_erc20    UInt8,
    is_erc721   UInt8
) ENGINE = MergeTree()
order by address;