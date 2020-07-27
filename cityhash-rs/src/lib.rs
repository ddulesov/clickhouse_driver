#![no_std]
#![allow(arithmetic_overflow)]
#![allow(clippy::many_single_char_names)]
#[cfg(test)]
extern crate clickhouse_driver_cth;
#[cfg(test)]
extern crate std;

use core::mem::size_of;
use core::ptr::read_unaligned;

#[derive(Debug, PartialEq)]
pub struct Pair(pub u64, pub u64);

/// Compare Hash with first 16 byte of Clickhouse Packet Header
/// @note it's work only on little endian system only
impl PartialEq<[u8; 16]> for Pair {
    fn eq(&self, other: &[u8; 16]) -> bool {
        (self.0 == fetch64(other)) && (self.1 == fetch64(&other[8..]))
    }
}

const K0: u64 = 0xc3a5c85c97cb3127;
const K1: u64 = 0xb492b66fbe98f273;
const K2: u64 = 0x9ae16a3b2f90404f;
const K3: u64 = 0xc949d7c7509e6557;

// #[inline(always)]
// fn rotate(val: u64, shift: u64) -> u64 {
//     if shift == 0 {
//         val
//     } else {
//         (val >> shift) | (val << (64 - shift))
//     }
// }

/// The same as rotate but `shift` must not be eq 0
#[inline(always)]
fn rotate_least(val: u64, shift: u64) -> u64 {
    (val >> shift) | (val << (64 - shift))
}

#[inline(always)]
fn shift_mix(val: u64) -> u64 {
    val ^ (val >> 47)
}

#[cfg(target_endian = "little")]
#[allow(clippy::cast_ptr_alignment)]
#[inline]
pub fn fetch64(src: &[u8]) -> u64 {
    debug_assert!(src.len() >= size_of::<u64>());
    let ptr = src.as_ptr() as *const u64;
    unsafe { read_unaligned(ptr) }
}

#[cfg(target_endian = "little")]
#[allow(clippy::cast_ptr_alignment)]
#[inline]
fn fetch32(src: &[u8]) -> u32 {
    debug_assert!(src.len() >= size_of::<u32>());
    let ptr = src.as_ptr() as *const u32;
    unsafe { read_unaligned(ptr) }
}

#[cfg(not(target_endian = "little"))]
#[allow(clippy::cast_ptr_alignment)]
#[inline]
fn fetch64(src: &[u8]) -> u64 {
    debug_assert!(src.len() >= mem::size_of::<u64>());
    let ptr = src.as_ptr() as *const u64;
    let src = unsafe { read_unaligned(ptr) };
    src.swap_bytes()
}

#[cfg(not(target_endian = "little"))]
#[allow(clippy::cast_ptr_alignment)]
#[inline]
fn fetch32(src: &[u8]) -> u32 {
    debug_assert!(src.len() >= size_of::<u32>());
    let ptr = src.as_ptr() as *const u32;
    let src = unsafe { read_unaligned(ptr) };
    src.swap_bytes()
}

fn hash_len0to16(src: &[u8]) -> u64 {
    if src.len() > 8 {
        let a: u64 = fetch64(src);
        let b: u64 = fetch64(&src[src.len() - 8..]);
        b ^ hash_len16(
            a,
            rotate_least(b.wrapping_add(src.len() as u64), src.len() as u64),
        )
    } else if src.len() >= 4 {
        let a = fetch32(src) as u64;
        hash_len16(
            (a << 3).wrapping_add(src.len() as u64),
            fetch32(&src[src.len() - 4..]) as u64,
        )
    } else if !src.is_empty() {
        let a: u8 = src[0];
        let b: u8 = src[src.len() >> 1];
        let c: u8 = src[src.len() - 1];
        let y: u64 = (a as u64).wrapping_add((b as u64) << 8);
        let z: u64 = (src.len() as u64).wrapping_add((c as u64) << 2);
        shift_mix(y.wrapping_mul(K2) ^ z.wrapping_mul(K3)).wrapping_mul(K2)
    } else {
        K2
    }
}

fn citymurmur(mut src: &[u8], seed: Pair) -> Pair {
    let mut a: u64 = seed.0;
    let mut b: u64 = seed.1;
    let mut c: u64;
    let mut d: u64;

    if src.len() <= 16 {
        a = shift_mix(a.wrapping_mul(K1)).wrapping_mul(K1);
        c = b.wrapping_mul(K1).wrapping_add(hash_len0to16(src));
        d = if src.len() >= 8 { fetch64(src) } else { c };
        d = shift_mix(a.wrapping_add(d));
    } else {
        c = hash_len16(fetch64(&src[src.len() - 8..]).wrapping_add(K1), a);
        d = hash_len16(
            b.wrapping_add(src.len() as u64),
            c.wrapping_add(fetch64(&src[src.len() - 16..])),
        );
        a = a.wrapping_add(d);
        loop {
            a ^= shift_mix(fetch64(src).wrapping_mul(K1)).wrapping_mul(K1);
            a = a.wrapping_mul(K1);
            b ^= a;
            c ^= shift_mix(fetch64(&src[8..]).wrapping_mul(K1)).wrapping_mul(K1);
            c = c.wrapping_mul(K1);
            d ^= c;
            src = &src[16..];
            if src.len() <= 16 {
                break;
            }
        }
    }

    a = hash_len16(a, c);
    b = hash_len16(d, b);
    Pair(a ^ b, hash_len16(b, a))
}

const KMUL: u64 = 0x9ddfea08eb382d69;

#[inline(always)]
fn hash_128to64(x: Pair) -> u64 {
    let mut a: u64 = (x.0 ^ x.1).wrapping_mul(KMUL);
    a = shift_mix(a);
    let mut b: u64 = (x.1 ^ a).wrapping_mul(KMUL);
    b = shift_mix(b);
    b.wrapping_mul(KMUL)
}

#[inline]
fn hash_len16(u: u64, v: u64) -> u64 {
    hash_128to64(Pair(u, v))
}

#[inline(always)]
#[allow(non_snake_case)]
fn _weakHashLen32WithSeeds(w: u64, x: u64, y: u64, z: u64, mut a: u64, mut b: u64) -> Pair {
    a = a.wrapping_add(w);
    b = rotate_least(b.wrapping_add(a).wrapping_add(z), 21);
    let c = a;
    a = a.wrapping_add(x).wrapping_add(y);
    b = b.wrapping_add(rotate_least(a, 44));
    Pair(a.wrapping_add(z), b.wrapping_add(c))
}

fn weak_hash_len32with_seeds(src: &[u8], a: u64, b: u64) -> Pair {
    _weakHashLen32WithSeeds(
        fetch64(src),
        fetch64(&src[8..]),
        fetch64(&src[16..]),
        fetch64(&src[24..]),
        a,
        b,
    )
}

fn cityhash128withseed(mut src: &[u8], seed: Pair) -> Pair {
    // We expect len >= 128 to be the common case.  Keep 56 bytes of state:
    // v, w, x, y, and z.
    let mut x: u64 = seed.0;
    let mut y: u64 = seed.1;

    let mut z: u64 = K1.wrapping_mul(src.len() as u64);
    let t: u64 = K1
        .wrapping_mul(rotate_least(y ^ K1, 49))
        .wrapping_add(fetch64(src));

    let mut v = Pair(
        t,
        K1.wrapping_mul(rotate_least(t, 42))
            .wrapping_add(fetch64(&src[8..])),
    );
    let mut w = Pair(
        K1.wrapping_mul(rotate_least(y.wrapping_add(z), 35))
            .wrapping_add(x),
        K1.wrapping_mul(rotate_least(x.wrapping_add(fetch64(&src[88..])), 53)),
    );

    // This is the same inner loop as CityHash64(), manually unrolled.
    loop {
        x = K1.wrapping_mul(rotate_least(
            x.wrapping_add(y)
                .wrapping_add(v.0)
                .wrapping_add(fetch64(&src[16..])),
            37,
        ));
        y = K1.wrapping_mul(rotate_least(
            y.wrapping_add(v.1).wrapping_add(fetch64(&src[48..])),
            42,
        ));
        x ^= w.1;
        y ^= v.0;
        z = rotate_least(z ^ w.0, 33);
        v = weak_hash_len32with_seeds(src, K1.wrapping_mul(v.1), x.wrapping_add(w.0));
        w = weak_hash_len32with_seeds(&src[32..], z.wrapping_add(w.1), y);
        core::mem::swap(&mut z, &mut x);

        //next 64 byte block
        src = &src[64..];

        x = K1.wrapping_mul(rotate_least(
            x.wrapping_add(y)
                .wrapping_add(v.0)
                .wrapping_add(fetch64(&src[16..])),
            37,
        ));
        y = K1.wrapping_mul(rotate_least(
            y.wrapping_add(v.1).wrapping_add(fetch64(&src[48..])),
            42,
        ));
        x ^= w.1;
        y ^= v.0;
        z = rotate_least(z ^ w.0, 33);
        v = weak_hash_len32with_seeds(src, K1.wrapping_mul(v.1), x.wrapping_add(w.0));
        w = weak_hash_len32with_seeds(&src[32..], z.wrapping_add(w.1), y);
        core::mem::swap(&mut z, &mut x);
        // next 64 bytes
        if src.len() < (128 + 64) {
            break;
        }
        src = &src[64..];
    }
    y = y.wrapping_add(K0.wrapping_mul(rotate_least(w.0, 37)).wrapping_add(z));
    x = x.wrapping_add(K0.wrapping_mul(rotate_least(v.0.wrapping_add(z), 49)));
    // If 0 < len < 128, hash up to 4 chunks of 32 bytes each from the end of s.
    while src.len() > 64 {
        y = K0
            .wrapping_mul(rotate_least(y.wrapping_sub(x), 42))
            .wrapping_add(v.1);
        w.0 = w.0.wrapping_add(fetch64(&src[src.len() - 16..]));
        x = K0.wrapping_mul(rotate_least(x, 49)).wrapping_add(w.0);
        w.0 = w.0.wrapping_add(v.0);
        v = weak_hash_len32with_seeds(&src[src.len() - 32..], v.0, v.1);
        src = &src[0..src.len() - 32];
    }
    // At this point our 48 bytes of state should contain more than
    // enough information for a strong 128-bit hash.  We use two
    // different 48-byte-to-8-byte hashes to get a 16-byte final result.
    x = hash_len16(x, v.0);
    y = hash_len16(y, w.0);

    Pair(
        hash_len16(x.wrapping_add(v.1), w.1).wrapping_add(y),
        hash_len16(x.wrapping_add(w.1), y.wrapping_add(v.1)),
    )
}

#[inline]
pub fn city_hash_128(src: &[u8]) -> Pair {
    if src.len() >= 144 {
        cityhash128withseed(&src[16..], Pair(fetch64(&src[..]) ^ K3, fetch64(&src[8..])))
    } else if src.len() >= 16 {
        citymurmur(&src[16..], Pair(fetch64(&src[..]) ^ K3, fetch64(&src[8..])))
    } else if src.len() >= 8 {
        citymurmur(
            &[],
            Pair(
                fetch64(&src[..]) ^ (K0.wrapping_mul(src.len() as u64)),
                fetch64(&src[src.len() - 8..]) ^ K1,
            ),
        )
    } else {
        citymurmur(src, Pair(K0, K1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clickhouse_driver_cth::{_CityHash128, _CityMurmur, c_char, Hash128};
    use std::vec::Vec;

    fn city_hash_ref(source: &[u8]) -> Pair {
        let h = unsafe { _CityHash128(source.as_ptr() as *const c_char, source.len()) };
        Pair(h.0, h.1)
    }

    fn city_murmur_ref(source: &[u8], seed: Pair) -> Pair {
        let h = unsafe {
            _CityMurmur(
                source.as_ptr() as *const c_char,
                source.len(),
                Hash128(seed.0, seed.1),
            )
        };
        Pair(h.0, h.1)
    }

    // fn hash_len_0to16_ref(source: &[u8]) -> u64 {
    //     unsafe { _HashLen0to16(source.as_ptr() as *const c_char, source.len()) }
    // }

    // #[test]
    // fn test_hash_len_0to16() {
    //     let src = b"";
    //     assert_eq!(hash_len_0to16_ref(src), hash_len0to16(src));
    //
    //     let src = b"4444";
    //     assert_eq!(hash_len_0to16_ref(src), hash_len0to16(src));
    //
    //     let src = b"999999999";
    //     assert_eq!(hash_len_0to16_ref(src), hash_len0to16(src));
    //
    //     let src = b"1234567890123456";
    //     assert_eq!(hash_len_0to16_ref(src), hash_len0to16(src));
    //
    //     let src = b"12";
    //     assert_eq!(hash_len_0to16_ref(src), hash_len0to16(src));
    //
    //     let src = b"abcdef";
    //     assert_eq!(hash_len_0to16_ref(src), hash_len0to16(src));
    //
    //     let src = b"0123456789abcdef";
    //     assert_eq!(hash_len_0to16_ref(src), hash_len0to16(src));
    // }

    #[test]
    fn test_cityhash_unaligned_load() {
        let src = [1u8, 0, 0, 1, 0, 0, 0, 0xFF, 0x1F, 0x1F, 0, 0];
        assert_eq!(fetch64(&src[..]), 0xFF00000001000001_u64);
    }

    // #[test]
    // fn test_cityhash_hash16() {
    //     let a: u64 = unsafe { testHashLen16(0xFF, 0xFF) };
    //     let b: u64 = hash_len16(0xFF, 0xFF);
    //     assert_eq!(a, b);
    //
    //     let a: u64 = unsafe { testHashLen16(0x1F, 0xFF) };
    //     let b: u64 = hash_len16(0x1F, 0xFF);
    //     assert_eq!(a, b);
    //
    //     let a: u64 = unsafe { testHashLen16(0x00, 0x7A) };
    //     let b: u64 = hash_len16(0x00, 0x7A);
    //     assert_eq!(a, b);
    //
    //     let a: u64 = unsafe { testHashLen16(0x00FF12A6, 0x7AF8375E) };
    //     let b: u64 = hash_len16(0x00FF12A6, 0x7AF8375E);
    //     assert_eq!(a, b);
    // }

    #[test]
    fn test_citymurmur() {
        assert_eq!(
            city_murmur_ref(b"", Pair(K0, K1)),
            citymurmur(b"", Pair(K0, K1))
        );
        assert_eq!(
            city_murmur_ref(b"0123456789", Pair(K0, K1)),
            citymurmur(b"0123456789", Pair(K0, K1))
        );
        assert_eq!(
            city_murmur_ref(b"0123456789abcdef", Pair(K0, K1)),
            citymurmur(b"0123456789abcdef", Pair(K0, K1))
        );
        let src = b"0123456789012345678901234567890123456789012345678901234567891234";
        assert_eq!(
            city_murmur_ref(src, Pair(K0, K1)),
            citymurmur(src, Pair(K0, K1))
        );
    }

    #[test]
    fn test_hash_128() {
        const MAX_SIZE: u32 = 1024 * 10;
        const ITER_COUNT: u8 = 5;
        use rand::Rng;
        for s in 8..MAX_SIZE {
            let mut b: Vec<u8> = Vec::with_capacity(s as usize);
            unsafe {
                b.set_len(s as usize);
            }
            for _ in 0..ITER_COUNT {
                rand::thread_rng().fill(&mut b[..]);
                assert_eq!(city_hash_ref(b.as_ref()), city_hash_128(b.as_ref()));
            }
        }
    }
}
