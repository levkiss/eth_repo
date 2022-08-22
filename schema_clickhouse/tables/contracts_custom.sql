CREATE TABLE STG_ETH.CONTRACTS (
    contract_address FixedString(70),
    contract_name String
)
ENGINE Log()