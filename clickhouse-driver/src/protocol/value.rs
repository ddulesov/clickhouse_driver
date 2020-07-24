use super::column::as_bytes_bufer;
use super::column::AsOutColumn;
use super::encoder::Encoder;
use crate::errors::{ConversionError, Result};
#[cfg(feature = "int128")]
use crate::types::Decimal128;
use crate::types::{Decimal, Decimal32, Decimal64, DecimalBits, Field, FieldMeta, SqlType, SCALE};
use byteorder::{LittleEndian, WriteBytesExt};
use chrono::{Date, DateTime, NaiveDate, NaiveDateTime, Utc};
use std::io;
use std::io::Write;
use std::net::{Ipv4Addr, Ipv6Addr};
use uuid::Uuid;

pub trait IntoColumn<'b>: Sized {
    fn to_column(this:  Vec<Self>) -> Box<dyn AsOutColumn+ 'b>;
}

lazy_static! {
    /// EPOCH is January 1, 1970 0:00:00 UTC (zero point for "UNIX timestamp").
    static ref EPOCH: chrono::NaiveDate  = {
         chrono::NaiveDate::from_ymd(1970,1,1)
    };
}

//TODO rename trait and method `to_column`
pub(crate) trait ToColumn {
    fn to_column(&self, field: &Field, writer: &mut dyn Write) -> Result<()>;
}

struct SimpleOutputColumn<T, F: Fn(&Field) -> bool> {
    data: Vec<T>,
    f: F,
}

impl<T, F> AsOutColumn for SimpleOutputColumn<T, F>
where
    T: ToColumn,
    F: Fn(&Field) -> bool,
{
    fn len(&self) -> usize {
        self.data.len()
    }

    fn encode(&self, field: &Field, writer: &mut dyn Write) -> Result<()> {
        for item in self.data.iter() {
            <T as ToColumn>::to_column(item, field, writer)?;
        }
        Ok(())
    }

    fn is_compatible(&self, field: &Field) -> bool {
        (&self.f)(field)
    }
}

pub trait NullValue {
    fn null() -> Self;
}

macro_rules! impl_null {
    ($t: ty, $v: expr) => {
        impl NullValue for $t {
            #[inline]
            fn null() -> Self {
                $v
            }
        }
    };
}
impl_null!(u8, 0u8);
impl_null!(i8, 0i8);
impl_null!(u16, 0u16);
impl_null!(i16, 0i16);
impl_null!(u32, 0u32);
impl_null!(i32, 0i32);
impl_null!(u64, 0u64);
impl_null!(i64, 0i64);
impl_null!(f32, 0f32);
impl_null!(f64, 0f64);

impl_null!(Ipv4Addr, Ipv4Addr::UNSPECIFIED);
impl_null!(Ipv6Addr, Ipv6Addr::UNSPECIFIED);
impl_null!(Uuid, Default::default());
impl_null!(Decimal32, Default::default());
impl_null!(Decimal64, Default::default());
#[cfg(feature = "int128")]
impl_null!(Decimal128, Default::default());
impl_null!(Date<Utc>, chrono::MIN_DATE);
impl_null!(DateTime<Utc>, chrono::MIN_DATE.and_hms(0, 0, 0));
impl_null!(&str, Default::default());
impl_null!(String, Default::default());

impl<T, F> AsOutColumn for SimpleOutputColumn<Option<T>, F>
where
    T: ToColumn + NullValue,
    F: Fn(&Field) -> bool,
{
    fn len(&self) -> usize {
        self.data.len()
    }
    /// Encodes null flags then encode data
    /// Null values are encoded as ordinary replaced by result of NullValue::null()
    fn encode(&self, field: &Field, writer: &mut dyn Write) -> Result<()> {
        for item in self
            .data
            .iter()
            .map(|item| if item.is_some() { 0u8 } else { 1u8 })
        {
            writer.write_u8(item)?;
        }
        let def: T = NullValue::null();
        for item in self.data.iter() {
            <T as ToColumn>::to_column(item.as_ref().unwrap_or(&def), field, writer)?;
        }
        Ok(())
    }

    fn is_compatible(&self, field: &Field) -> bool {
        (&self.f)(field)
    }
}

/// Default (used by nullable string) implementation
impl ToColumn for &str {
    #[inline]
    fn to_column(&self, field: &Field, writer: &mut dyn Write) -> Result<()> {
        let slice = std::slice::from_ref(self);
        match field.sql_type {
            SqlType::String => encode_string(slice, writer),
            SqlType::FixedString(val) => encode_fixedstring(slice, val, writer),
            _ => unreachable!(),
        }
    }
}
impl ToColumn for String {
    #[inline]
    fn to_column(&self, field: &Field, writer: &mut dyn Write) -> Result<()> {
        let slice = std::slice::from_ref(self);
        match field.sql_type {
            SqlType::String => encode_string(slice, writer),
            SqlType::FixedString(val) => encode_fixedstring(slice, val, writer),
            _ => unreachable!(),
        }
    }
}

/// Encode string array. Column `String` `Native Format`
/// |StringLength as VarInt (0..9 bytes)|String byte array  | ... next item
fn encode_string<T: AsRef<[u8]>>(data: &[T], writer: &mut dyn Write) -> Result<()> {
    for s in data {
        let s = s.as_ref();
        (s.len() as u64).encode(writer)?;
        s.encode(writer)?;
    }
    Ok(())
}
/// Encode fixed length string array. Column `FixedString` `Native Format`
/// |String byte array|...next item
fn encode_fixedstring<T: AsRef<[u8]>>(data: &[T], size: u32, writer: &mut dyn Write) -> Result<()> {
    for s in data {
        let s = s.as_ref();
        //empty or default string workaround
        if s.is_empty() {
            for _ in 0..size {
                writer.write_u8(0)?;
            }
        } else if s.len() != size as usize {
            return Err(ConversionError::FixedStringLengthNotMatch(size).into());
        } else {
            writer.write_all(s)?;
        }
    }

    Ok(())
}

fn encode_enum8<T: AsRef<[u8]>>(
    data: &[T],
    meta: &FieldMeta,
    writer: &mut dyn Write,
) -> Result<()> {
    for s in data {
        let val = meta.str2val(s.as_ref())?;
        writer.write_i8(val as i8)?;
    }
    Ok(())
}

fn encode_enum16<T: AsRef<[u8]>>(
    data: &[T],
    meta: &FieldMeta,
    writer: &mut dyn Write,
) -> Result<()> {
    for s in data {
        let val: i16 = meta.str2val(s.as_ref())?;
        writer.write_i16::<LittleEndian>(val)?;
    }
    Ok(())
}

struct StringOutputColumn<T> {
    data: Vec<T>,
}

impl<'a, T> AsOutColumn for StringOutputColumn<T>
where
    T: AsRef<[u8]>,
{
    fn len(&self) -> usize {
        self.data.len()
    }

    fn encode(&self, field: &Field, writer: &mut dyn Write) -> Result<()> {
        match field.sql_type {
            SqlType::String => encode_string(self.data.as_ref(), writer),
            SqlType::FixedString(v) => encode_fixedstring(self.data.as_ref(), v, writer),
            SqlType::Enum8 => encode_enum8(
                self.data.as_ref(),
                field.get_meta().expect("enum index corrupted"),
                writer,
            ),
            SqlType::Enum16 => encode_enum16(
                self.data.as_ref(),
                field.get_meta().expect("enum index corrupted"),
                writer,
            ),
            _ => unreachable!(),
        }
    }

    fn is_compatible(&self, field: &Field) -> bool {
        matches!(field.sql_type, SqlType::String | SqlType::FixedString(_) | SqlType::Enum8 | SqlType::Enum16)        
    }
}

impl ToColumn for Ipv4Addr {
    fn to_column(&self, _field: &Field, writer: &mut dyn Write) -> Result<()> {
        let mut b = self.octets();
        b.reverse();

        writer.write_all(&b[..]).map_err(Into::into)
    }
}

impl ToColumn for Ipv6Addr {
    fn to_column(&self, _field: &Field, writer: &mut dyn Write) -> Result<()> {
        let b = self.octets();
        writer.write_all(&b[..]).map_err(Into::into)
    }
}

impl ToColumn for Uuid {
    fn to_column(&self, _field: &Field, writer: &mut dyn Write) -> Result<()> {
        let i = self.as_u128();
        writer.write_u64::<LittleEndian>((i >> 64) as u64)?;
        writer
            .write_u64::<LittleEndian>(i as u64)
            .map_err(Into::into)
    }
}

impl ToColumn for Date<Utc> {
    fn to_column(&self, _field: &Field, writer: &mut dyn Write) -> Result<()> {
        let days = (self.naive_utc() - *EPOCH).num_days();

        if days < 0 || days > u16::MAX as i64 {
            return Err(ConversionError::UnsupportedConversion.into());
        }

        let days = (days as u16).to_le_bytes();
        writer.write_all(&days[..]).map_err(Into::into)
    }
}

impl ToColumn for DateTime<Utc> {
    fn to_column(&self, field: &Field, writer: &mut dyn Write) -> Result<()> {
        let mut timestamp = self.timestamp();

        match field.sql_type {
            SqlType::DateTime => {
                if timestamp < 0 || timestamp > u32::MAX as i64 {
                    return Err(ConversionError::UnsupportedConversion.into());
                }
                let timestamp = (timestamp as u32).to_le_bytes();
                writer.write_all(&timestamp[..]).map_err(Into::into)
            }
            SqlType::DateTime64(p, _) => {
                debug_assert!(p < 9);
                //TODO refine getting value
                timestamp *= SCALE[p as usize];
                writer
                    .write_i64::<LittleEndian>(timestamp)
                    .map_err(Into::into)
            }
            _ => unreachable!(),
        }
    }
}

macro_rules! to_column_discrete {
    ($t:ty, $f: ident, $endian: ty) => {
        impl ToColumn for $t {
            #[inline]
            fn to_column(&self, _field: &Field, writer: &mut dyn Write) -> Result<()> {
                writer.$f::<$endian>(*self).map_err(Into::into)
            }
        }
    };
    ($t:ty, $f: ident) => {
        impl ToColumn for $t {
            #[inline]
            fn to_column(&self, _field: &Field, writer: &mut dyn Write) -> Result<()> {
                writer.$f(*self).map_err(Into::into)
            }
        }
    };
}

to_column_discrete!(i8, write_i8);
to_column_discrete!(u8, write_u8);
to_column_discrete!(i16, write_i16, LittleEndian);
to_column_discrete!(u16, write_u16, LittleEndian);
to_column_discrete!(i32, write_i32, LittleEndian);
to_column_discrete!(u32, write_u32, LittleEndian);
to_column_discrete!(i64, write_i64, LittleEndian);
to_column_discrete!(u64, write_u64, LittleEndian);

#[cfg(feature = "int128")]
to_column_discrete!(i128, write_i128, LittleEndian);
#[cfg(feature = "int128")]
to_column_discrete!(u128, write_u128, LittleEndian);

to_column_discrete!(f32, write_f32, LittleEndian);
to_column_discrete!(f64, write_f64, LittleEndian);

impl<T: ToColumn + DecimalBits> ToColumn for Decimal<T> {
    fn to_column(&self, field: &Field, writer: &mut dyn Write) -> Result<()> {
        if let SqlType::Decimal(p, s) = field.sql_type {
            debug_assert!(T::fit(p));
            if s != self.scale {
                return Err(ConversionError::UnsupportedConversion.into());
            }
        } else {
            unreachable!()
        }
        self.underlying.to_column(field, writer)
    }
}

/// Some data types u(i)8,16,32,64 f32, f64 have the same
/// representation  in memory and in Clickhouse columnar data format
/// so they can be easily encoded all at once
struct BinaryCompatibleOutColumn<T: Sized> {
    sql_type: SqlType,
    data: Vec<T>,
}

fn encode_data_bc(data: &[u8], writer: &mut dyn Write) -> io::Result<()> {
    writer.write_all(data)
}

impl<'a, T: Sized + Send + Sync> AsOutColumn for BinaryCompatibleOutColumn<T> {
    fn len(&self) -> usize {
        self.data.len()
    }
    fn encode(&self, _field: &Field, writer: &mut dyn Write) -> Result<()> {
        encode_data_bc(unsafe { as_bytes_bufer(self.data.as_ref()) }, writer).map_err(Into::into)
    }
    fn is_compatible(&self, field: &Field) -> bool {
        self.sql_type == field.sql_type
    }
}

macro_rules! impl_intocolumn_bc {
    ($fs: ty, $sql: path) => {
        impl<'b> IntoColumn<'b> for $fs {
            fn to_column(this: Vec<$fs>) -> Box<dyn AsOutColumn +'b > {
                Box::new(BinaryCompatibleOutColumn {
                    data: this,
                    sql_type: $sql,
                })
            }
        }
    };
}

macro_rules! impl_intocolumn_simple {
    ($fs: ty, $sql: expr) => {
        impl<'b> IntoColumn<'b> for $fs
        where $fs: 'b{
            fn to_column(this: Vec<$fs>) -> Box<dyn AsOutColumn +'b > {
                Box::new(SimpleOutputColumn {
                    data: this,
                    f: $sql,
                })
            }
        }
    };
}

macro_rules! impl_intocolumn_string {
    ($fs: ty) => {
        impl<'b> IntoColumn<'b> for $fs
        where $fs: 'b{
            fn to_column(this: Vec<$fs>) -> Box<dyn AsOutColumn +'b> {
                Box::new(StringOutputColumn { data: this })
            }
        }
    };
}

impl_intocolumn_bc!(u8, SqlType::UInt8);
impl_intocolumn_bc!(i8, SqlType::Int8);
impl_intocolumn_bc!(u16, SqlType::UInt16);
impl_intocolumn_bc!(i16, SqlType::Int16);
impl_intocolumn_bc!(u32, SqlType::UInt32);
impl_intocolumn_bc!(i32, SqlType::Int32);
impl_intocolumn_bc!(u64, SqlType::UInt64);
impl_intocolumn_bc!(i64, SqlType::Int64);
impl_intocolumn_bc!(f64, SqlType::Float64);
impl_intocolumn_bc!(f32, SqlType::Float32);
impl_intocolumn_bc!(ValueDate, SqlType::Date);
impl_intocolumn_bc!(ValueDateTime, SqlType::DateTime);

impl_intocolumn_simple!(Ipv4Addr, |f| f.sql_type == SqlType::Ipv4);
impl_intocolumn_simple!(Ipv6Addr, |f| f.sql_type == SqlType::Ipv6);
impl_intocolumn_simple!(Date<Utc>, |f| f.sql_type == SqlType::Date);

impl_intocolumn_simple!(Decimal32, |f| {
    match f.sql_type {
        SqlType::Decimal(p, _) => i32::fit(p),
        _ => false,
    }
});

impl_intocolumn_simple!(Decimal64, |f| {
    match f.sql_type {
        SqlType::Decimal(p, _) => i64::fit(p),
        _ => false,
    }
});

#[cfg(feature = "int128")]
impl_intocolumn_simple!(Decimal128, |f| {
    match f.sql_type {
        SqlType::Decimal(p, _) => i128::fit(p),
        _ => false,
    }
});

// TODO make available to insert into DateTime64
impl_intocolumn_simple!(DateTime<Utc>, |f| matches!(f.sql_type, SqlType::DateTime | SqlType::DateTime64(..)) );

impl_intocolumn_simple!(Uuid, |f| f.sql_type == SqlType::Uuid);
impl_intocolumn_string!(&'b str);
impl_intocolumn_string!(String);

//Nullable types
impl_intocolumn_simple!(Option<u8>, |f| f.sql_type == SqlType::UInt8);
impl_intocolumn_simple!(Option<i8>, |f| f.sql_type == SqlType::Int8);
impl_intocolumn_simple!(Option<u16>, |f| f.sql_type == SqlType::UInt16);
impl_intocolumn_simple!(Option<i16>, |f| f.sql_type == SqlType::Int16);
impl_intocolumn_simple!(Option<u32>, |f| f.sql_type == SqlType::UInt32);
impl_intocolumn_simple!(Option<i32>, |f| f.sql_type == SqlType::Int32);
impl_intocolumn_simple!(Option<u64>, |f| f.sql_type == SqlType::UInt64);
impl_intocolumn_simple!(Option<i64>, |f| f.sql_type == SqlType::Int64);
impl_intocolumn_simple!(Option<f32>, |f| f.sql_type == SqlType::Float32);
impl_intocolumn_simple!(Option<f64>, |f| f.sql_type == SqlType::Float64);
impl_intocolumn_simple!(Option<Uuid>, |f| f.sql_type == SqlType::Uuid);
impl_intocolumn_simple!(Option<Ipv4Addr>, |f| f.sql_type == SqlType::Ipv4);
impl_intocolumn_simple!(Option<Ipv6Addr>, |f| f.sql_type == SqlType::Ipv6);
impl_intocolumn_simple!(Option<Date<Utc>>, |f| f.sql_type == SqlType::Date);
impl_intocolumn_simple!(Option<DateTime<Utc>>, |f| f.sql_type == SqlType::DateTime);

impl_intocolumn_simple!(Option<&'b str>, |f| f.sql_type == SqlType::String);
impl_intocolumn_simple!(Option<String>, |f| f.sql_type == SqlType::String);

impl_intocolumn_simple!(Option<Decimal32>, |f| {
    match f.sql_type {
        SqlType::Decimal(p, _) => i32::fit(p),
        _ => false,
    }
});

impl_intocolumn_simple!(Option<Decimal64>, |f| {
    match f.sql_type {
        SqlType::Decimal(p, _) => i64::fit(p),
        _ => false,
    }
});

#[cfg(feature = "int128")]
impl_intocolumn_simple!(Option<Decimal128>, |f| {
    match f.sql_type {
        SqlType::Decimal(p, _) => i128::fit(p),
        _ => false,
    }
});

#[derive(Copy, Clone, Debug)]
pub struct ValueIp4([u8; 4]);

impl Into<Ipv4Addr> for ValueIp4 {
    fn into(mut self) -> Ipv4Addr {
        self.0.reverse();
        self.0.into()
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ValueIp6([u8; 16]);

impl Into<Ipv6Addr> for ValueIp6 {
    fn into(mut self) -> Ipv6Addr {
        self.0.reverse();
        self.0.into()
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ValueUuid([u8; 16]);

impl Into<Uuid> for ValueUuid {
    fn into(mut self) -> Uuid {
        self.0[0..8].reverse();
        self.0[8..16].reverse();
        Uuid::from_bytes(self.0)
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ValueDate(pub [u8; 2]);

#[derive(Copy, Clone, Debug)]
pub struct ValueDateTime(pub [u8; 4]);

#[derive(Copy, Clone, Debug)]
pub struct ValueDateTime64(pub i64);

#[derive(Copy, Clone, Debug)]
pub struct ValueDecimal32(pub i32, pub u8, pub u8);

#[derive(Copy, Clone, Debug)]
pub struct ValueDecimal64(pub i64, pub u8, pub u8);

#[cfg(feature = "int128")]
#[derive(Copy, Clone, Debug)]
pub struct ValueDecimal128(pub i128, pub u8, pub u8);

impl ValueDate {
    pub(super) fn to_date(&self) -> chrono::Date<chrono::offset::Utc> {
        let ce: i32 = 719163_i32 + i16::from_le_bytes(self.0) as i32;
        let nt = NaiveDate::from_num_days_from_ce(ce);
        chrono::Date::from_utc(nt, Utc)
    }
    #[allow(dead_code)]
    pub fn from_days(day: i16) -> ValueDate {
        ValueDate(day.to_le_bytes())
    }
}

impl ValueDateTime {
    pub(super) fn to_datetime(&self) -> DateTime<chrono::offset::Utc> {
        let sec = i32::from_le_bytes(self.0) as i64;
        let nt = NaiveDateTime::from_timestamp(sec, 0);
        DateTime::from_utc(nt, Utc)
    }

    pub fn from_timestamp(timestamp: i32) -> ValueDateTime {
        ValueDateTime(timestamp.to_le_bytes())
    }
}

impl ValueDateTime64 {
    pub(super) fn to_datetime(self, precision: u8) -> DateTime<chrono::offset::Utc> {
        let magnitude = SCALE[precision as usize];
        let sec = self.0.wrapping_div(magnitude);
        //TODO: verify correctness for dates below 1970-01-01, that is negative self.0 value
        let nsec = self.0.wrapping_rem(magnitude) * SCALE[(9 - precision) as usize];

        let nt = NaiveDateTime::from_timestamp(sec, nsec as u32);
        DateTime::from_utc(nt, Utc)
    }

    pub fn from_raw(base: i64) -> ValueDateTime64 {
        ValueDateTime64(base)
    }
}
