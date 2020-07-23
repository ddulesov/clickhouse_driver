use crate::errors::{ConversionError, DriverError, Result};
use core::marker::PhantomData;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt};

pub(crate) struct ReadBool<'a, R: ?Sized> {
    inner: &'a mut R,
}

impl<'a, R: AsyncRead> ReadBool<'a, R> {
    fn poll_get(&mut self, cx: &mut Context<'_>) -> Poll<Result<u8>> {
        let mut b = [0u8; 1];
        {
            let inner = unsafe { Pin::new_unchecked(&mut *self.inner) };

            if 0 == ready!(inner.poll_read(cx, &mut b)?) {
                return Poll::Ready(Err(DriverError::BrokenData.into()));
            };
        }
        Ok(b[0]).into()
    }
}

impl<'a, R: AsyncRead> Future for ReadBool<'a, R> {
    type Output = Result<u8>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        me.poll_get(cx)
    }
}

pub(crate) struct ReadVString<'a, T: FromBytes, R> {
    length_: usize,
    data: Vec<u8>,
    inner: Pin<&'a mut R>,
    _marker: PhantomData<&'a T>,
}

pub trait FromBytes: Sized {
    fn from_bytes(bytes: Vec<u8>) -> Result<Self>;
}

impl FromBytes for String {
    fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        String::from_utf8(bytes).map_err(|_e| ConversionError::Utf8.into())
    }
}

impl FromBytes for Vec<u8> {
    #[inline]
    fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        Ok(bytes)
    }
}

impl<'a, T: FromBytes, R: AsyncRead> ReadVString<'a, T, R> {
    pub(crate) fn new(reader: &'a mut R, length: usize) -> ReadVString<'a, T, R> {
        let data = unsafe {
            let mut v = Vec::with_capacity(length);
            v.set_len(length);
            v
        };
        let inner = unsafe { Pin::new_unchecked(reader) };
        ReadVString {
            length_: 0,
            data,
            inner,
            _marker: PhantomData,
        }
    }

    fn poll_get(&mut self, cx: &mut Context<'_>) -> Poll<Result<T>> {
        loop {
            if self.length_ == self.data.len() {
                let s = std::mem::replace(&mut self.data, Vec::new());
                let s: Result<T> = FromBytes::from_bytes(s);
                self.length_ = 0;
                return s.into();
            };
            self.length_ += ready!(self
                .inner
                .as_mut()
                .poll_read(cx, &mut self.data[self.length_..])?);
        }
    }
}

impl<'a, T: FromBytes, R: AsyncRead> Future for ReadVString<'a, T, R> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        me.poll_get(cx)
    }
}

pub(crate) struct ReadVInt<'a, R> {
    value: u64,
    i: u8,
    inner: Pin<&'a mut R>,
}

impl<'a, R: AsyncRead> ReadVInt<'a, R> {
    fn new(reader: &'a mut R) -> ReadVInt<'a, R> {
        let inner = unsafe { Pin::new_unchecked(reader) };
        ReadVInt {
            value: 0,
            i: 0,
            inner,
        }
    }

    fn poll_get(&mut self, cx: &mut Context<'_>) -> Poll<Result<u64>> {
        let mut b = [0u8; 1];
        loop {
            //let inner: Pin<&mut R> =  unsafe{ Pin::new_unchecked(self.inner) };
            if 0 == ready!(self.inner.as_mut().poll_read(cx, &mut b)?) {
                return Poll::Ready(Err(DriverError::BrokenData.into()));
            }
            let b = b[0];

            self.value |= ((b & 0x7f) as u64) << (self.i);
            self.i += 7;

            if b < 0x80 {
                return Poll::Ready(Ok(self.value));
            };

            if self.i > 63 {
                return Poll::Ready(Err(DriverError::BrokenData.into()));
            };
        }
    }
}

impl<'a, R: AsyncRead> Future for ReadVInt<'a, R> {
    type Output = Result<u64>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        me.poll_get(cx)
    }
}

pub struct ValueReader<R> {
    inner: R,
}

impl<R: AsyncRead> ValueReader<R> {
    pub(super) fn new(reader: R) -> ValueReader<R> {
        ValueReader { inner: reader }
    }
    //TODO: Optimize read using bufered data
    pub(super) fn read_vint(&mut self) -> ReadVInt<'_, R> {
        ReadVInt::new(&mut self.inner)
    }
    //TODO: Optimize read using bufered data
    pub(super) fn read_string<T: FromBytes>(&mut self, len: u64) -> ReadVString<'_, T, R> {
        ReadVString::new(&mut self.inner, len as usize)
    }

    #[inline]
    pub(super) fn as_mut(&mut self) -> &mut R {
        &mut self.inner
    }
}

impl<R: AsyncRead + Unpin> ValueReader<R> {
    pub(super) async fn skip(&mut self, len: u64) -> Result<()> {
        let mut buf = [0u8; 64];
        let mut about_to_read = len as usize;
        while about_to_read > 0 {
            let l = std::cmp::min(about_to_read, 64);
            about_to_read -= self.inner.read(&mut buf[0..l]).await?;
        }
        Ok(())
    }

    pub(super) async fn read_byte(&mut self) -> Result<u8> {
        let mut buf = [0u8; 1];
        self.inner.read_exact(&mut buf[..]).await?;

        Ok(buf[0])
    }
}
