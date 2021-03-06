CREATE TABLE main(
    i64 Int64,
    u64 UInt64,
    i32 Int32, 
    u32 UInt32,
    i16 Int16, 
    u16 UInt16,   
    i8 Int8,
    u8 UInt8,
    f32 Float32,
    f64 Float64,
    title String,
    lcs LowCardinality(String),
    mon FixedString(3),
    d Date,
    t DateTime,
    dt64 DateTime64(3,'Europe/Moscow'),
    uuid UUID,
    e8 Enum8('yes'=1, 'no'=0, 'n/a'=-1),
    e16 Enum16('rust'=10,'go'=1,'c++'=2,'python'=3,'java'=4,'c#'=5),
    ip4 IPv4,
    ip6 IPv6,
    n Nullable(UInt16),
    dm1 Decimal(9,2), dm2 Decimal(12,6) 
) ENGINE=MergeTree() ORDER BY d PARTITION BY i64