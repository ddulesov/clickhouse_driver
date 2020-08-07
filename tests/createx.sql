CREATE TABLE mainx(i64 Int64,
    a8 Array(Int8),
    a16 Array(Int16),
    a32 Array(UInt32),
    a64 Array(UInt64),
    a8d Array(Array(UInt8)),
    aip4 Array(IPv4),
    aip6 Array(IPv6),
    ad Array(Date),
    adt Array(DateTime),
    adc Array(Decimal(9,2)),
    lcs LowCardinality(Nullable(String))
) ENGINE=MergeTree() ORDER BY i64 PARTITION BY i64