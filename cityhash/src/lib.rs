#![no_std]
extern crate libc;
#[cfg(test)]
#[macro_use]
extern crate std;

use core::mem::size_of;
use core::ops::Deref;
use core::ptr::read_unaligned;
pub use libc::c_char;

#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct Hash128(pub u64, pub u64);

impl Hash128 {
    #[cfg(int128)]
    #[inline(always)]
    fn to_u128(self) -> u128 {
        (self.0 as u128) << 64 | self.1 as u128
    }
}

impl Deref for Hash128 {
    type Target = [u8; 16];
    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { &*(self as *const Hash128 as *const [u8; 16]) }
    }
}

#[allow(clippy::cast_ptr_alignment)]
#[inline]
pub fn fetch64(src: &[u8]) -> u64 {
    debug_assert!(src.len() >= size_of::<u64>());
    let ptr = src.as_ptr() as *const u64;
    unsafe { read_unaligned(ptr) }
}

#[allow(clippy::cast_ptr_alignment)]
#[inline]
pub fn fetch128(src: &[u8]) -> u128 {
    debug_assert!(src.len() >= size_of::<u128>());
    let ptr = src.as_ptr() as *const u128;
    unsafe { read_unaligned(ptr) }
}

#[cfg(not(int128))]
impl PartialEq<&[u8]> for Hash128 {
    fn eq(&self, other: &&[u8]) -> bool {
        other.len() == 16 && (self.0 == fetch64(*other)) && (self.1 == fetch64(&other[8..]))
    }
}

#[cfg(int128)]
impl PartialEq<&[u8]> for Pair {
    fn eq(&self, other: &&[u8]) -> bool {
        (other.len() == 16) && (self.to_u128() == fetch128(other))
    }
}

extern "C" {
    pub fn _CityHash128(s: *const c_char, len: usize) -> Hash128;
    pub fn _CityHash64(s: *const c_char, len: usize) -> u64;
    pub fn _CityMurmur(s: *const c_char, len: usize, seed: Hash128) -> Hash128;
}

#[cfg(test)]
extern "C" {
    // pub fn _HashLen16(u: u64, v: u64) -> u64;
    // pub fn _Fetch64(s: *const c_char) -> u64;
    // pub fn _HashLen0to16(s: *const c_char, len: usize) -> u64;
}

pub fn city_hash_128(source: &[u8]) -> Hash128 {
    unsafe { _CityHash128(source.as_ptr() as *const c_char, source.len()) }
}

pub fn city_hash_64(source: &[u8]) -> u64 {
    unsafe { _CityHash64(source.as_ptr() as *const c_char, source.len()) }
}

#[cfg(test)]
mod test {
    use crate::*;

    #[test]
    fn test_city_hash_128() {
        assert_eq!(
            city_hash_128(b"abc"),
            [
                0xfe, 0x48, 0x77, 0x57, 0x95, 0xf1, 0x0f, 0x90, 0x7e, 0x0d, 0xb2, 0x55, 0x63, 0x17,
                0xa9, 0x13
            ]
            .as_ref()
        );
        assert_ne!(
            city_hash_128(b"abc"),
            [
                0x00, 0x48, 0x77, 0x57, 0x95, 0xf1, 0x0f, 0x90, 0x7e, 0x0d, 0xb2, 0x55, 0x63, 0x17,
                0xa9, 0x13
            ]
            .as_ref()
        );
        assert_eq!(
            city_hash_128(b"01234567890abc"),
            [
                0x36, 0x20, 0xe9, 0x1b, 0x54, 0x23, 0x04, 0xbe, 0x2d, 0xc7, 0x32, 0x8d, 0x93, 0xd2,
                0x3b, 0x89
            ]
            .as_ref()
        );
        assert_eq!(
            city_hash_128(b"01234567890123456789012345678901234567890123456789012345678901234"),
            [
                0x24, 0xd7, 0xd5, 0xdc, 0x8e, 0xb6, 0x85, 0xb2, 0xb1, 0xd9, 0x78, 0x15, 0xa2, 0x2a,
                0xb0, 0x3d
            ]
            .as_ref()
        );
        assert_eq!(
            city_hash_128(b""),
            [
                0x2b, 0x9a, 0xc0, 0x64, 0xfc, 0x9d, 0xf0, 0x3d, 0x29, 0x1e, 0xe5, 0x92, 0xc3, 0x40,
                0xb5, 0x3c
            ]
            .as_ref()
        );

        assert_ne!(
            city_hash_128(b"abc"),
            [
                0xfe, 0x48, 0x77, 0x57, 0x95, 0xf1, 0x0f, 0x90, 0x7e, 0x0d, 0xb2, 0x55, 0x63, 0x17,
                0xa9, 0x11
            ]
            .as_ref()
        );
    }
}
