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
#[cfg(feature = "int128")]
use super::value::ValueDecimal128;
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
use std::convert::{TryFrom, TryInto};
use std::fmt::Formatter;

macro_rules! err {
    ($err: expr) => {
        Err($err.into())
    };
}

pub trait AsOutColumn {
    fn len(&self) -> usize;
    fn encode(&self, field: &Field, writer: &mut dyn Write) -> Result<()>;
    fn is_compatible(&self, field: &Field) -> bool;
}

/// Convert received from Clickhouse server data to rust type
pub trait AsInColumn: Send {
    unsafe fn get_at(&self, index: u64) -> ValueRef<'_>;
}
/// Output block column
pub struct ColumnDataAdapter<'b> {
    pub(crate) name: &'b str,
    pub(crate) flag: u8,
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
                    err!(ConversionError::UnsupportedConversion)
                }
            }
            Some(ValueRefEnum::Date(v)) => {
                let d = v.to_date();
                Ok(Some(d.and_hms(0, 0, 0)))
            }
            _ => err!(ConversionError::UnsupportedConversion),
        }
    }
}
/// Implement SqlType::Enum(x)|String|FixedSring(size)->&str data conversion
impl<'a> Value<'a, &'a str> for ValueRef<'a> {
    fn get(&'a self, field: &'a Field) -> Result<Option<&'_ str>> {
        match self.inner {
            Some(ValueRefEnum::String(v)) => Ok(Some(from_utf8(v)?)),
            Some(ValueRefEnum::Enum(v)) => {
                let meta = field.get_meta().expect("corrupted enum index");
                let title = v.transcode(meta);
                Ok(Some(from_utf8(title)?))
            }
            _ => err!(ConversionError::UnsupportedConversion),
        }
    }
}

/// Implement SqlType::Enum(x)|String|FixedSring(size) - > &[u8]
impl<'a> Value<'a, &'a [u8]> for ValueRef<'a> {
    fn get(&'a self, field: &'a Field) -> Result<Option<&'_ [u8]>> {
        match self.inner {
            Some(ValueRefEnum::String(v)) => Ok(Some(v)),
            Some(ValueRefEnum::Enum(v)) => {
                let meta = field.get_meta().expect("corrupted enum index");
                let title = v.transcode(meta);
                Ok(Some(title))
            }
            _ => err!(ConversionError::UnsupportedConversion),
        }
    }
}

#[inline]
fn decimal_scale_from_field(field: &Field) -> u8 {
    match field.sql_type {
        SqlType::Decimal(_, s) => s,
        _ => 0, //unreachable
    }
}

/// Implement SqlType::Decimal32 -> Decimal<i32> data conversion
impl<'a> Value<'a, Decimal32> for ValueRef<'a> {
    fn get(&'a self, field: &'a Field) -> Result<Option<Decimal32>> {
        match self.inner {
            Some(ValueRefEnum::Decimal32(v)) => {
                let scale = decimal_scale_from_field(field);
                Ok(Some(Decimal::from(v.0, scale)))
            }
            _ => err!(ConversionError::UnsupportedConversion),
        }
    }
}
/// Implement SqlType::Decimal64 -> Decimal<i64> data conversion
impl<'a> Value<'a, Decimal64> for ValueRef<'a> {
    fn get(&'a self, field: &'a Field) -> Result<Option<Decimal64>> {
        match self.inner {
            Some(ValueRefEnum::Decimal64(v)) => {
                let scale = decimal_scale_from_field(field);
                Ok(Some(Decimal::from(v.0, scale)))
            }
            _ => err!(ConversionError::UnsupportedConversion),
        }
    }
}
/// Implement SqlType::Decimal128 -> Decimal<i128> data conversion
#[cfg(feature = "int128")]
impl<'a> Value<'a, Decimal128> for ValueRef<'a> {
    fn get(&'a self, field: &'a Field) -> Result<Option<Decimal128>> {
        match self.inner {
            Some(ValueRefEnum::Decimal128(v)) => {
                let scale = decimal_scale_from_field(field);
                Ok(Some(Decimal::from(v.0, scale)))
            }
            _ => err!(ConversionError::UnsupportedConversion),
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
                    _ => err!(ConversionError::UnsupportedConversion),
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

// SqlType::X t-> X
impl_value!(u64, ValueRefEnum::UInt64);
impl_value!(i64, ValueRefEnum::Int64);
impl_value!(u32, ValueRefEnum::UInt32);
impl_value!(i32, ValueRefEnum::Int32);
impl_value!(u16, ValueRefEnum::UInt16);
impl_value!(i16, ValueRefEnum::Int16);
impl_value!(u8, ValueRefEnum::UInt8);
impl_value!(i8, ValueRefEnum::Int8);

/// An implementation provides Row-to-Object deserialization interface
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
            .map(|c| c.data.get_at(row_index))
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
    #[inline]
    unsafe fn get_at(&self, index: u64) -> ValueRef<'_> {
        self.as_ref().get_at(index)
    }
}

pub(crate) type BoxString = Box<[u8]>;
/*
/// Column of String, FixedString
pub(crate) struct StringColumn {
    data: Vec<BoxString>,
}

pub(crate) struct StringNullColumn{
    inner: StringColumn,
    pub(crate) nulls: Vec<u8>,
}

impl StringColumn {
    pub(crate) fn set_nulls(self:  Self, nulls: Option<Vec<u8>>)->Box<dyn AsInColumn>{
        if let Some(nulls) = nulls {
            Box::new( StringNullColumn{inner: self, nulls} )
        }else{
            Box::new( self)
        }
    }

    pub(crate) async fn load_string_column<R: AsyncBufRead + Unpin>(
        reader: R,
        rows: u64
    ) -> Result<StringColumn> {
        let mut data: Vec<BoxString> = Vec::with_capacity(rows as usize);

        let mut rdr = ValueReader::new(reader);
        let mut l: u64;
        for _ in 0..rows {
            l = rdr.read_vint().await?;
            let s: Vec<u8> = rdr.read_string(l).await?;
            data.push(s.into_boxed_slice());
        }

        Ok(StringColumn { data })
    }

    pub(crate) async fn load_fixed_string_column<R: AsyncBufRead + Unpin>(
        mut reader: R,
        rows: u64,
        width: u32
    ) -> Result<StringColumn> {
        let mut data: Vec<BoxString> = Vec::with_capacity(rows as usize);

        for _ in 0..rows {
            let mut s: Vec<u8> = Vec::with_capacity(width as usize);
            unsafe {
                s.set_len(width as usize);
            }
            reader.read_exact(s.as_mut_slice()).await?;
            data.push(s.into_boxed_slice());
        }

        Ok(StringColumn { data })
    }
}

impl AsInColumn for StringColumn {

    unsafe fn get_at(&self, index: u64, _: &Field) -> ValueRef<'_> {
        assert!((index as usize) < self.data.len());
        let vr = self.data.get_unchecked(index as usize);
        //let vr = self.data[index as usize];

        ValueRef {
            inner: Some(ValueRefEnum::String(vr)),
        }
    }
}

impl AsInColumn for StringNullColumn {

    unsafe fn get_at<'a>(&'a self, index: u64, field: &'a Field) -> ValueRef<'a> {
        debug_assert!((index as usize) < self.nulls.len());
        if *(self.nulls.get_unchecked( index as usize ))==1{
            ValueRef {
                inner: None
            }
        }else {
            self.inner.get_at(index, &field )
        }
    }
}
*/

/// Enum value, String index pair,
/// 0-T, clickhouse value
/// 1-BoxString, enum string value
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
    /// Format Enum index value as a string that represent enum metadata
    #[allow(unused_must_use)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use std::fmt::Write;
        f.write_str("'")?;
        // SAFETY! all Enum string values are received from Server as a
        // part of Enum metadata. So, all titles  is valid utf-8.
        let s = unsafe { self.as_str() };
        s.escape_default().for_each(|c| {
            f.write_char(c);
        });
        f.write_str("' = ")?;
        f.write_fmt(format_args!("{}", self.0))
    }
}

pub trait EnumTranscode: Copy {
    /// Perform Enum value to Enum title conversion
    fn transcode(self, meta: &FieldMeta) -> &[u8];
}

// interpret it as *T (i8, u8) (i16, u16) in relation to input type
// TODO: return u16::MAX on any error
/// transform enum value to index in title array
impl EnumTranscode for i16 {
    #[inline]
    fn transcode(self, meta: &FieldMeta) -> &[u8] {
        meta.val2str(self)
    }
}

/// Array of Enum8 or Enum16 values
pub(crate) struct EnumColumn<T> {
    data: Vec<T>,
}

/// T can be 'u8'(Enum8) or 'u16'(Enum16)
impl<T: Send> EnumColumn<T> {
    /// Read server stream as a sequence of u8(u16) bytes
    /// and store them in internal buffer
    pub(crate) async fn load_column<R: AsyncBufRead + Unpin>(
        mut reader: R,
        rows: u64,
        field: &Field,
    ) -> Result<EnumColumn<T>> {
        debug_assert!(field.get_meta().is_some());

        let mut data: Vec<T> = Vec::with_capacity(rows as usize);
        unsafe {
            data.set_len(rows as usize);
            reader.read_exact(as_bytes_bufer_mut(&mut data)).await?;
        }
        Ok(EnumColumn { data })
    }
    pub(crate) fn set_nulls(self, _nulls: Option<Vec<u8>>) -> Box<EnumColumn<T>> {
        // TODO: If Enum can be nullable how it stores nulls?
        Box::new(self)
    }
}

impl<'a, T: Copy + Send + Into<i16>> AsInColumn for EnumColumn<T> {
    /// Perform conversion of Enum value to corresponding reference to title.
    /// Store the reference  in ValueRef struct
    unsafe fn get_at(&self, index: u64) -> ValueRef<'_> {
        assert!((index as usize) < self.data.len());
        let enum_value: i16 = (*self.data.get_unchecked(index as usize)).into();

        ValueRef {
            inner: Some(ValueRefEnum::Enum(enum_value)),
        }
    }
}
/// Fixed sized type column
pub(crate) struct FixedColumn<T: Sized> {
    data: Vec<T>,
}

pub(crate) struct FixedNullColumn<T: Sized> {
    inner: FixedColumn<T>,
    nulls: Vec<u8>,
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

pub(crate) type StringColumn = FixedColumn<BoxString>;

impl FixedColumn<BoxString> {
    pub(crate) async fn load_string_column<R: AsyncBufRead + Unpin>(
        reader: R,
        rows: u64,
    ) -> Result<StringColumn> {
        let mut data: Vec<BoxString> = Vec::with_capacity(rows as usize);

        let mut rdr = ValueReader::new(reader);
        let mut l: u64;
        for _ in 0..rows {
            l = rdr.read_vint().await?;
            let s: Vec<u8> = rdr.read_string(l).await?;
            data.push(s.into_boxed_slice());
        }

        Ok(StringColumn { data })
    }

    pub(crate) async fn load_fixed_string_column<R: AsyncBufRead + Unpin>(
        mut reader: R,
        rows: u64,
        width: u32,
    ) -> Result<StringColumn> {
        let mut data: Vec<BoxString> = Vec::with_capacity(rows as usize);

        for _ in 0..rows {
            let mut s: Vec<u8> = Vec::with_capacity(width as usize);
            unsafe {
                s.set_len(width as usize);
            }
            reader.read_exact(s.as_mut_slice()).await?;
            data.push(s.into_boxed_slice());
        }

        Ok(StringColumn { data })
    }
}

impl AsInColumn for FixedColumn<BoxString> {
    unsafe fn get_at(&self, index: u64) -> ValueRef<'_> {
        assert!((index as usize) < self.data.len());
        let vr = self.data.get_unchecked(index as usize);

        ValueRef {
            inner: Some(ValueRefEnum::String(vr)),
        }
    }
}

impl<T: Sized> FixedColumn<T> {
    pub(crate) async fn load_column<R: AsyncBufRead + Unpin>(
        mut reader: R,
        rows: u64,
    ) -> Result<FixedColumn<T>> {
        let mut data: Vec<T> = Vec::with_capacity(rows as usize);

        unsafe {
            data.set_len(rows as usize);
            reader.read_exact(as_bytes_bufer_mut(&mut data)).await?;
        }

        Ok(FixedColumn { data })
    }

    pub(crate) fn cast<U: Sized>(self: FixedColumn<T>) -> FixedColumn<U> {
        assert_eq!(size_of::<T>(), size_of::<U>());
        assert!(align_of::<T>() >= align_of::<U>());

        unsafe {
            let mut clone = std::mem::ManuallyDrop::new(self);
            FixedColumn {
                data: Vec::from_raw_parts(
                    clone.data.as_mut_ptr() as *mut U,
                    clone.data.len(),
                    clone.data.capacity(),
                ),
            }
        }
    }
}

impl<T: Sized + 'static> FixedColumn<T>
where
    FixedNullColumn<T>: AsInColumn,
    FixedColumn<T>: AsInColumn,
{
    pub(crate) fn set_nulls(self: Self, nulls: Option<Vec<u8>>) -> Box<dyn AsInColumn> {
        if let Some(nulls) = nulls {
            Box::new(FixedNullColumn { inner: self, nulls })
        } else {
            Box::new(self)
        }
    }
}

impl<T: Sized> AsInColumn for FixedNullColumn<T>
where
    FixedColumn<T>: AsInColumn,
{
    unsafe fn get_at(&self, index: u64) -> ValueRef<'_> {
        debug_assert!((index as usize) < self.nulls.len());
        if self.nulls[index as usize] == 1 {
            ValueRef { inner: None }
        } else {
            self.inner.get_at(index)
        }
    }
}

macro_rules! impl_fixed_column {
    ($f:ty,$vr:expr) => {
        impl AsInColumn for FixedColumn<$f> {
            unsafe fn get_at(&self, index: u64) -> ValueRef<'_> {
                debug_assert!((index as usize) < self.data.len());
                let vr = self.data.get_unchecked(index as usize);
                ValueRef {
                    inner: Some($vr(*vr)),
                }
            }
        }
    };
}
#[allow(dead_code)]
pub(crate) struct FixedArrayColumn<T> {
    data: Vec<T>,
    index: Vec<usize>,
    //phantom: PhantomData<T>
}

impl<T: Send> AsInColumn for FixedArrayColumn<T> {
    unsafe fn get_at(&self, index: u64) -> ValueRef<'_> {
        let size1 = if index == 0 {
            0_usize
        } else {
            *self.index.get_unchecked((index - 1) as usize)
        };

        let size2 = *self.index.get_unchecked(index as usize);
        debug_assert!(size1 <= size2);

        ValueRef {
            //inner: Some( ValueRefEnum::Array( &self.data[size1..size2] ) )
            inner: None,
        }
    }
}

pub(crate) struct LowCardinalityColumn<T: Sized + Send> {
    values: Vec<BoxString>,
    data: Vec<T>,
}
// TODO: rewrite for 32-bit system
impl<T> LowCardinalityColumn<T>
where
    T: Sized + Ord + Copy + Send + 'static,
    usize: TryFrom<T>,
{
    pub(crate) fn empty() -> Box<dyn AsInColumn> {
        Box::new(LowCardinalityColumn {
            values: Vec::new(),
            data: Vec::<u8>::new(),
        })
    }

    pub(crate) async fn load_column<R>(
        reader: R,
        rows: u64,
        values: FixedColumn<BoxString>,
    ) -> Result<Box<dyn AsInColumn>>
    where
        R: AsyncBufRead + Unpin,
    {
        let data: FixedColumn<T> = FixedColumn::load_column(reader, rows).await?;

        let m = data
            .data
            .iter()
            .max()
            .expect("corrupted lowcardinality column");
        let m: usize = (*m).try_into().unwrap_or_else(|_| 0);
        if m >= values.data.len() {
            return err!(DriverError::IntegrityError);
        }

        Ok(Box::new(LowCardinalityColumn {
            data: data.data,
            values: values.data,
        }))
    }
}

impl<T: Copy + Send + Sized + TryInto<usize>> AsInColumn for LowCardinalityColumn<T> {
    unsafe fn get_at(&self, index: u64) -> ValueRef<'_> {
        debug_assert!((index as usize) < self.data.len());
        let index = self.data.get_unchecked(index as usize);
        let index: usize = (*index).try_into().unwrap_or_else(|_| 0);
        if index == 0 {
            ValueRef { inner: None }
        } else {
            let v = self.values.get_unchecked(index);
            ValueRef {
                inner: Some(ValueRefEnum::String(v)),
            }
        }
    }
}

// FixedColumn implementations
impl_fixed_column!(u8, ValueRefEnum::UInt8);
impl_fixed_column!(i8, ValueRefEnum::Int8);
impl_fixed_column!(u16, ValueRefEnum::UInt16);
impl_fixed_column!(i16, ValueRefEnum::Int16);
impl_fixed_column!(u32, ValueRefEnum::UInt32);
impl_fixed_column!(i32, ValueRefEnum::Int32);
impl_fixed_column!(u64, ValueRefEnum::UInt64);
impl_fixed_column!(i64, ValueRefEnum::Int64);
#[cfg(feature = "int128")]
impl_fixed_column!(u128, ValueRefEnum::UInt128);

impl_fixed_column!(f32, ValueRefEnum::Float32);
impl_fixed_column!(f64, ValueRefEnum::Float64);

impl_fixed_column!(ValueUuid, ValueRefEnum::Uuid);
impl_fixed_column!(ValueIp4, ValueRefEnum::Ip4);
impl_fixed_column!(ValueIp6, ValueRefEnum::Ip6);

impl_fixed_column!(ValueDecimal32, ValueRefEnum::Decimal32);
impl_fixed_column!(ValueDecimal64, ValueRefEnum::Decimal64);
#[cfg(feature = "int128")]
impl_fixed_column!(ValueDecimal128, ValueRefEnum::Decimal128);

impl_fixed_column!(ValueDate, ValueRefEnum::Date);
impl_fixed_column!(ValueDateTime, ValueRefEnum::DateTime);
impl_fixed_column!(ValueDateTime64, ValueRefEnum::DateTime64);
