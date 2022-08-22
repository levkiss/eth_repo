CREATE TABLE DM_ETH.DAU_TTL
(
    contract FixedString(70),
    avg_dau_last_month Int32,
    avg_new_dau_last_month Int32,
    monthly_change_dau Float32,
    monthly_change_new_dau Float32,
    quarter_change_new_dau Float32,
    timestamp_date DateTime
)
    ENGINE = MergeTree()
ORDER BY toYYYYMMDD(timestamp_date)
partition by toYYYYMMDD(timestamp_date);
