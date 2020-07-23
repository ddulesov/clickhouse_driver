use std::borrow::Borrow;
use std::fmt;
use std::io::Write;
use std::mem::{align_of, size_of};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::slice::Iter;
use std::str::from_utf8;

use chrono::{DateTime, Utc};
use tokio::io::{AsyncBufRead, AsyncReadExt};
use uuid::Uuid;

use super::block::{BlockColumn, ServerBlock};
use super::decoder::ValueReader;
use super::value::{
    ValueDate, ValueDateTime, ValueDateTime64, ValueDecimal32, ValueDecimal64, ValueIp4, ValueIp6,
    ValueUuid,
};

use super::{Value, ValueRefEnum};
use crate::errors::{ConversionError, DriverError, Result};
#[cfg(feature = "int128")]
use crate::types::Decimal128;
use crate::types::{Decimal, Decimal32, Decimal64, Field, FieldMeta, SqlType};

use std::cmp::Ordering;
use std::fmt::Formatter;

pub trait AsOutColumn {
    fn len(&self) -> usize;
    fn encode(&self, field: &Field, writer: &mut dyn Write) -> Result<()>;
    fn is_compatible(&self, field: &Field) -> bool;
}

/// Convert received from Clickhouse server data to rust type
pub trait AsInColumn: Send {
    fn len(&self) -> usize;
    unsafe fn get_at<'a>(&'a self, index: u64, field: &'a Field) -> ValueRef<'a>;
}
/// Output block column
pub struct ColumnDataAdapter<'b> {
    pub(crate) name: &'b str,
    pub(crate) nullable: bool,
    pub(crate) data: Box<dyn AsOutColumn + 'b>,
}

/// Hold reference (or value for discreet data types ) to Clickhouse row values
/// Option::None value indicate null
#[derive(Debug)]
pub struct ValueRef<'a> {
    inner: Option<ValueRefEnum<'a>>,
}
/// Implement SqlType::DateTime,SqlType::DateTime64-> chrono::DateTime<Utc> data conversion
impl<'a> Value<'a, DateTime<Utc>> for ValueRef<'a> {
    fn get(&'a self, field: &'a Field) -> Result<Option<DateTime<Utc>>> {
        match self.inner {
            Some(ValueRefEnum::DateTime(v)) => Ok(Some(v.to_datetime())),
            Some(ValueRefEnum::DateTime64(v)) => {
                if let SqlType::DateTime64(p, _) = field.sql_type {
                    Ok(Some(v.to_datetime(p)))
                } else {
                    // TODO: Apparently not reachable. Replace it with notreachable!
                    Err(ConversionError::UnsupportedConversion.into())
                }
            }
            Some(ValueRefEnum::Date(v)) => {
                let d = v.to_date();
                Ok(Some(d.and_hms(0, 0, 0)))
            }
            _ => Err(ConversionError::UnsupportedConversion.into()),
        }
    }
}
/// Implement SqlType::String->&str data conversion
impl<'a> Value<'a, &'a str> for ValueRef<'a> {
    fn get(&'a self, _: &'a Field) -> Result<Option<&'_ str>> {
        match self.inner {
            Some(ValueRefEnum::String(v)) => Ok(Some(from_utf8(v)?)),
            _ => Err(ConversionError::UnsupportedConversion.into()),
        }
    }
}
/// Implement SqlType::Decimal32 -> Decimal<i32> data conversion
impl<'a> Value<'a, Decimal32> for ValueRef<'a> {
    fn get(&'a self, _: &'a Field) -> Result<Option<Decimal32>> {
        match self.inner {
            Some(ValueRefEnum::Decimal32(v)) => Ok(Some(Decimal::from_parts(v.0, v.1, v.2))),
            _ => Err(ConversionError::UnsupportedConversion.into()),
        }
    }
}
/// Implement SqlType::Decimal64 -> Decimal<i64> data conversion
impl<'a> Value<'a, Decimal64> for ValueRef<'a> {
    fn get(&'a self, _: &'a Field) -> Result<Option<Decimal64>> {
        match self.inner {
            Some(ValueRefEnum::Decimal64(v)) => Ok(Some(Decimal::from_parts(v.0, v.1, v.2))),
            _ => Err(ConversionError::UnsupportedConversion.into()),
        }
    }
}
/// Implement SqlType::Decimal128 -> Decimal<i128> data conversion
#[cfg(feature = "int128")]
impl<'a> Value<'a, Decimal128> for ValueRef<'a> {
    fn get(&'a self, _: &'a Field) -> Result<Option<Decimal128>> {
        match self.inner {
            Some(ValueRefEnum::Decimal128(v)) => Ok(Some(Decimal::from_parts(v.0, v.1, v.2))),
            _ => Err(ConversionError::UnsupportedConversion.into()),
        }
    }
}

macro_rules! impl_value {
    ($f:ty, $vr:path) => {
        impl<'a> Value<'a, $f> for ValueRef<'a> {
            fn get(&'a self, _: &'a Field) -> Result<Option<$f>> {
                match self.inner {
                    Some($vr(v)) => Ok(Some(v.into())),
                    None => Ok(None),
                    _ => Err(ConversionError::UnsupportedConversion.into()),
                }
            }
        }
    };
}
// Implement common types data conversion
// SqlType::X -> rust data conversion
impl_value!(f32, ValueRefEnum::Float32);
impl_value!(f64, ValueRefEnum::Float64);
// SqlType::Ipv4 - > Ipv4Addr
impl_value!(Ipv4Addr, ValueRefEnum::Ip4);
// SqlType::Ipv6 - > Ipv6Addr
impl_value!(Ipv6Addr, ValueRefEnum::Ip6);
// SqlType::UUID - > Uuid
impl_value!(Uuid, ValueRefEnum::Uuid);
// SqlType::Enum(x)|String|FixedSring(size) - > &[u8]
impl_value!(&'a [u8], ValueRefEnum::String);

impl_value!(u64, ValueRefEnum::UInt64);
impl_value!(i64, ValueRefEnum::Int64);
impl_value!(u32, ValueRefEnum::UInt32);
impl_value!(i32, ValueRefEnum::Int32);
impl_value!(u16, ValueRefEnum::UInt16);
impl_value!(i16, ValueRefEnum::Int16);
impl_value!(u8, ValueRefEnum::UInt8);
impl_value!(i8, ValueRefEnum::Int8);

/// An implementation provides Row to Object deserialization interface
/// It's used internally by block iterator
///
/// # Example
/// Some(object) = block.iter()
pub trait Deserialize: Sized {
    fn deserialize(row: Row) -> Result<Self>;
}
/// Input Block data row
#[derive(Debug)]
pub struct Row<'a> {
    /// vector of data references
    col: Vec<ValueRef<'a>>,
    /// data store
    block: &'a ServerBlock,
}

impl<'a> Row<'a> {
    /// # Safety
    /// This function should be called after
    /// `row_index` parameter was checked against row array boundary
    /// Block Iterators check it
    pub unsafe fn create(block: &'a ServerBlock, row_index: u64) -> Row<'a> {
        let col: Vec<_> = block
            .columns
            .iter()
            .map(|c| {
                let n = c
                    .nulls
                    .as_ref()
                    .map_or(0, |nulls| nulls[row_index as usize]);
                if n == 1 {
                    ValueRef { inner: None }
                } else {
                    c.data.get_at(row_index, &c.header.field)
                }
            })
            .collect();
        Row { col, block }
    }
    /// Returns the number of columns
    /// This number must correspond to the number of fields in the SELECT statement
    #[inline]
    pub fn len(&self) -> usize {
        self.col.len()
    }
    /// Empty server Data block is the special type of message.
    /// It's used internally and usually cannot be returned to user
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.col.is_empty()
    }

    pub fn iter_columns(&self) -> Iter<BlockColumn> {
        self.block.columns.iter()
    }

    pub fn iter_values(&self) -> Iter<ValueRef<'_>> {
        self.col.iter()
    }
    /// Returns row field value converting underlying Sql type
    /// to rust data type if the specific conversion is available.
    /// Otherwise it returns ConversionError
    /// For nullable Sql types if the field contains null value this method
    /// returns Ok(None)
    pub fn value<T>(&'a self, index: usize) -> Result<Option<T>>
    where
        T: 'a,
        ValueRef<'a>: Value<'a, T>,
    {
        let value_ref = self.col.get(index).ok_or(DriverError::IndexOutOfRange)?;
        let field = self
            .block
            .columns
            .get(index)
            .expect("column index out of range")
            .header
            .field
            .borrow();

        value_ref.get(field)
    }
    /// The same as `value` method but without performing any checking.
    /// # Safety
    /// Calling this method with an out of bound 'index' value is UB.
    /// Panic if this method is called with unsupported data conversion
    /// At the moment the driver provides limited number of data conversions.
    /// This method should be used only if you know that table data structure
    /// will nether change and you know query resulted data types.
    pub unsafe fn value_unchecked<T>(&'a self, index: usize) -> Option<T>
    where
        T: 'a,
        ValueRef<'a>: Value<'a, T>,
    {
        assert!(self.col.len() > index);
        let value_ref = self.col.get_unchecked(index);
        let field = self
            .block
            .columns
            .get_unchecked(index)
            .header
            .field
            .borrow();

        value_ref.get(field).expect("Conversion error")
    }

    #[inline]
    pub fn column_descr(&self, index: usize) -> Option<&BlockColumn> {
        self.block.columns.get(index)
    }
    /// Returns column index by its name
    pub fn column_index(&self, name: &str) -> usize {
        let item = self
            .block
            .columns
            .iter()
            .enumerate()
            .find(|(_i, c)| c.header.name.eq(name))
            .unwrap();
        item.0
    }
    /// Perform transformation Row to Plain object.
    /// Requires that object type implements Deserialize trait
    pub fn deserialize<D: Deserialize>(self) -> Result<D> {
        <D as Deserialize>::deserialize(self)
    }
}

impl<'a, C: AsInColumn + ?Sized + 'a> AsInColumn for Box<C> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    unsafe fn get_at<'b>(&'b self, index: u64, field: &'b Field) -> ValueRef<'b> {
        self.as_ref().get_at(index, field)
    }
}

type BoxString = Box<[u8]>;

pub(crate) struct StringColumn {
    data: Vec<BoxString>,
}

impl StringColumn {
    pub(crate) async fn load_string_column<R: AsyncBufRead + Unpin>(
        reader: R,
        rows: u64,
    ) -> Result<Box<StringColumn>> {
        let mut data: Vec<BoxString> = Vec::with_capacity(rows as usize);

        let mut rdr = ValueReader::new(reader);
        let mut l: u64;
        for _ in 0..rows {
            l = rdr.read_vint().await?;
            let s: Vec<u8> = rdr.read_string(l).await?;
            data.push(s.into_boxed_slice());
        }

        Ok(Box::new(StringColumn { data }))
    }

    pub(crate) async fn load_fixed_string_column<R: AsyncBufRead + Unpin>(
        mut reader: R,
        rows: u64,
        width: u32,
    ) -> Result<Box<StringColumn>> {
        let mut data: Vec<BoxString> = Vec::with_capacity(rows as usize);

        for _ in 0..rows {
            let mut s: Vec<u8> = Vec::with_capacity(width as usize);
            unsafe {
                s.set_len(width as usize);
            }
            reader.read_exact(s.as_mut_slice()).await?;
            data.push(s.into_boxed_slice());
        }

        Ok(Box::new(StringColumn { data }))
    }
}

impl AsInColumn for StringColumn {
    fn len(&self) -> usize {
        self.data.len()
    }
    unsafe fn get_at(&self, index: u64, _: &Field) -> ValueRef<'_> {
        assert!((index as usize) < self.data.len());

        let vr = self.data.get_unchecked(index as usize);
        let vr = vr.as_ref();

        ValueRef {
            inner: Some(ValueRefEnum::String(vr)),
        }
    }
}

/// Enum value , String index pair,
/// 0-clickhouse value, 1-string index in data vector
#[derive(Clone)]
pub struct EnumIndex<T>(pub T, pub BoxString);

impl<T> EnumIndex<T> {
    #[inline]
    pub(crate) unsafe fn as_str(&self) -> &str {
        std::str::from_utf8_unchecked(self.1.as_ref())
    }
}

impl<T: Ord + Copy> EnumIndex<T> {
    /// Sort by enum value (key)
    #[inline]
    pub(crate) fn fn_sort_val(item1: &EnumIndex<T>) -> T {
        item1.0
    }
    /// Sort by enum name
    #[inline]
    pub(crate) fn fn_sort_str(item1: &EnumIndex<T>, item2: &EnumIndex<T>) -> Ordering {
        Ord::cmp(item1.1.as_ref(), &item2.1.as_ref())
    }
}

impl<T: PartialEq> PartialEq for EnumIndex<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: Copy + fmt::Display> fmt::Display for EnumIndex<T> {
    #[allow(unused_must_use)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use std::fmt::Write;
        f.write_str("'")?;
        // SAFETY! all Enum string values got from Server as plain string.
        let s = unsafe { self.as_str() };
        s.escape_default().for_each(|c| {
            f.write_char(c);
        });
        //f.write_str(s);
        f.write_str("' = ")?;
        f.write_fmt(format_args!("{}", self.0))
    }
}

pub trait EnumTranscode: Copy {
    fn transcode(self, meta: &FieldMeta) -> &[u8];
}

// TODO: make  FieldMeta.index typeless
// interpret it as *T (i8, u8) (i16, u16) in relation to input type
// TODO: return u16::MAX on any error
/// transform enum value to index in title array
impl EnumTranscode for i16 {
    #[inline]
    fn transcode(self, meta: &FieldMeta) -> &[u8] {
        meta.val2str(self)
    }
}

pub(crate) struct EnumColumn<T> {
    data: Vec<T>,
}

/// T can be 'u8' or 'u16'
impl<T: Send> EnumColumn<T> {
    pub(crate) async fn load_column<R: AsyncBufRead + Unpin>(
        mut reader: R,
        rows: u64,
        field: &Field,
    ) -> Result<Box<EnumColumn<T>>> {
        debug_assert!(field.get_meta().is_some());

        let mut data: Vec<T> = Vec::with_capacity(rows as usize);
        unsafe {
            data.set_len(rows as usize);
            reader.read_exact(as_bytes_bufer_mut(&mut data)).await?;
        }
        Ok(Box::new(EnumColumn { data }))
    }
}

impl<'a, T: Copy + Send + Into<i16>> AsInColumn for EnumColumn<T> {
    fn len(&self) -> usize {
        self.data.len()
    }

    unsafe fn get_at<'b>(&'b self, index: u64, field: &'b Field) -> ValueRef<'b> {
        assert!((index as usize) < self.data.len());
        let meta = match field.get_meta() {
            Some(meta) => meta,

            // TODO: Apparently it's unreachanble code and should  rewrite using
            // unreachable! macro
            None => return ValueRef { inner: None },
        };

        let enum_value: i16 = (*self.data.get_unchecked(index as usize)).into(); //[index as usize];
        let title = enum_value.transcode(meta);

        ValueRef {
            inner: Some(ValueRefEnum::String(title)),
        }
    }
}
/// Fixed sized type column
pub(crate) struct FixedColumn<T: Sized> {
    data: Vec<T>,
}

#[inline]
pub unsafe fn as_bytes_bufer_mut<T>(v: &mut [T]) -> &mut [u8] {
    std::slice::from_raw_parts_mut(
        v.as_mut_ptr() as *mut u8,
        v.len() * std::mem::size_of::<T>(),
    )
}

#[inline]
pub unsafe fn as_bytes_bufer<T>(v: &[T]) -> &[u8] {
    std::slice::from_raw_parts(v.as_ptr() as *mut u8, v.len() * std::mem::size_of::<T>())
}

impl<T: Sized> FixedColumn<T> {
    pub(crate) async fn load_column<R: AsyncBufRead + Unpin>(
        mut reader: R,
        rows: u64,
    ) -> Result<Box<FixedColumn<T>>> {
        let mut data: Vec<T> = Vec::with_capacity(rows as usize);

        unsafe {
            data.set_len(rows as usize);
            reader.read_exact(as_bytes_bufer_mut(&mut data)).await?;
        }

        Ok(Box::new(FixedColumn { data }))
    }

    pub(crate) fn cast<U: Sized>(self: Box<FixedColumn<T>>) -> Box<FixedColumn<U>> {
        assert_eq!(size_of::<T>(), size_of::<U>());
        assert!(align_of::<T>() >= align_of::<U>());

        unsafe {
            let mut clone = std::mem::ManuallyDrop::new(self);

            Box::new(FixedColumn {
                data: Vec::from_raw_parts(
                    clone.data.as_mut_ptr() as *mut U,
                    clone.data.len(),
                    clone.data.capacity(),
                ),
            })
        }
    }
}

macro_rules! impl_fixed_column {
    ($f:ty,$vr:expr) => {
        impl AsInColumn for FixedColumn<$f> {
            #[inline]
            fn len(&self) -> usize {
                self.data.len()
            }
            unsafe fn get_at(&self, index: u64, _: &Field) -> ValueRef<'_> {
                assert!((index as usize) < self.data.len());
                let vr = self.data[index as usize];
                ValueRef {
                    inner: Some($vr(vr)),
                }
            }
        }
    };
}

impl_fixed_column!(u8, ValueRefEnum::UInt8);
impl_fixed_column!(i8, ValueRefEnum::Int8);
impl_fixed_column!(u16, ValueRefEnum::UInt16);
impl_fixed_column!(i16, ValueRefEnum::Int16);
impl_fixed_column!(u32, ValueRefEnum::UInt32);
impl_fixed_column!(i32, ValueRefEnum::Int32);
impl_fixed_column!(u64, ValueRefEnum::UInt64);
impl_fixed_column!(i64, ValueRefEnum::Int64);

impl_fixed_column!(u128, ValueRefEnum::UInt128);

impl_fixed_column!(f32, ValueRefEnum::Float32);
impl_fixed_column!(f64, ValueRefEnum::Float64);

impl_fixed_column!(ValueUuid, ValueRefEnum::Uuid);
impl_fixed_column!(ValueIp4, ValueRefEnum::Ip4);
impl_fixed_column!(ValueIp6, ValueRefEnum::Ip6);

impl_fixed_column!(ValueDecimal32, ValueRefEnum::Decimal32);
impl_fixed_column!(ValueDecimal64, ValueRefEnum::Decimal64);

impl_fixed_column!(ValueDate, ValueRefEnum::Date);
impl_fixed_column!(ValueDateTime, ValueRefEnum::DateTime);
impl_fixed_column!(ValueDateTime64, ValueRefEnum::DateTime64);
