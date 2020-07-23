#![cfg(rustc_nightly)]
#![feature(test)]
#![feature(core_intrinsics)]

extern crate test;

#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::_mm_crc32_u64;
//use core::intrinsics::size_of;
use test::Bencher;

use rand::prelude::*;

use clickhouse_cityhash::{_CityHash128, c_char};
use clickhouse_cityhash_rs::*;

#[inline]
fn city_hash_ref(source: &[u8]) -> Pair {
    let h = unsafe { _CityHash128(source.as_ptr() as *const c_char, source.len()) };
    Pair(h.0, h.1)
}

#[target_feature(enable = "sse4.2")]
unsafe fn crc32_64(mut src: &[u8]) -> u64 {
    let mut seed: u64 = 0;
    while src.len() >= 8 {
        let a = fetch64(src);
        seed = _mm_crc32_u64(seed, a);
        src = &src[8..];
    }

    if !src.is_empty() {
        let mut buf = [0u8; 8];
        let s = std::cmp::min(8, src.len());

        buf[0..s].copy_from_slice(&src[0..s]);
        let a = u64::from_le_bytes(buf);
        seed = _mm_crc32_u64(seed, a);
    }

    seed
}

fn data(len: usize) -> Vec<u8> {
    let mut b: Vec<u8> = Vec::with_capacity(len as usize);
    unsafe {
        b.set_len(len as usize);
    }

    rand::thread_rng().fill(&mut b[..]);
    b
}

#[bench]
fn bench_1k_rust(b: &mut Bencher) {
    let input = data(1024);
    let mut out = city_hash_128(&input[..]);
    b.iter(|| {
        out = city_hash_128(&input[..]);
    });
}

#[bench]
fn bench_1k_cpp(b: &mut Bencher) {
    let input = data(1024);
    let mut out = city_hash_ref(&input[..]);
    b.iter(|| {
        out = city_hash_ref(&input[..]);
    });
}

#[bench]
fn bench_64k_rust(b: &mut Bencher) {
    let input = data(64 * 1024);
    let mut out = city_hash_128(&input[..]);
    b.iter(|| {
        out = city_hash_128(&input[..]);
    });
}

#[bench]
fn bench_64k_cpp(b: &mut Bencher) {
    let input = data(64 * 1024);
    let mut out = city_hash_ref(&input[..]);
    b.iter(|| {
        out = city_hash_ref(&input[..]);
    });
}

#[bench]
fn bench_1m_rust(b: &mut Bencher) {
    let input = data(1024 * 1024);
    let mut out = city_hash_128(&input[..]);
    b.iter(|| {
        out = city_hash_128(&input[..]);
    });
}

#[bench]
fn bench_1m_cpp(b: &mut Bencher) {
    let input = data(1024 * 1024);
    let mut out = city_hash_ref(&input[..]);
    b.iter(|| {
        out = city_hash_ref(&input[..]);
    });
}

#[bench]
#[cfg(feature = "simd")]
fn bench_64k_rust_city_crc(b: &mut Bencher) {
    let input = data(64 * 1024);
    let mut out = unsafe { city_crc(&input[..], 0) };
    b.iter(|| {
        out = unsafe { city_crc(&input[..], 0) };
    });
}

#[bench]
fn bench_64k_rust_crc32_64(b: &mut Bencher) {
    let input = data(64 * 1024);
    let mut out = unsafe { crc32_64(&input[..]) };
    b.iter(|| {
        out = unsafe { crc32_64(&input[..]) };
    });
}
