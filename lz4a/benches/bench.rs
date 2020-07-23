#![feature(test)]
#![feature(core_intrinsics)]

extern crate libc;
extern crate test;

use test::Bencher;

use libc::c_char;
use rand::prelude::*;

//use core::intrinsics::size_of;
use lz4a::*;

fn data(len: usize) -> Vec<u8> {
    let mut b: Vec<u8> = Vec::with_capacity(len as usize);
    unsafe {
        b.set_len(len as usize);
    }

    b
}

#[bench]
fn bench_compress_static_1m(b: &mut Bencher) {
    let mut input = data(1024 * 1024);
    rand::thread_rng().fill(&mut input[..]);

    let cz: usize = LZ4_CompressInplaceBufferSize(1024 * 1024);
    let mut output: Vec<u8> = Vec::with_capacity(cz);
    unsafe {
        output.set_len(cz as usize);
    }
    let mut code: i32 = 0;

    b.iter(|| {
        code = unsafe {
            LZ4_compress_default(
                input.as_ptr() as *const c_char,
                output.as_mut_ptr() as *mut c_char,
                (input.len()) as i32,
                output.len() as i32,
            )
        };
    });
}

#[bench]
fn bench_decompress_static_1m(b: &mut Bencher) {
    let mut input = data(1024 * 1024);
    rand::thread_rng().fill(&mut input[..]);

    let cz: usize = LZ4_CompressInplaceBufferSize(1024 * 1024);
    let mut output: Vec<u8> = Vec::with_capacity(cz);

    unsafe {
        output.set_len(cz as usize);
    }

    let mut code = unsafe {
        LZ4_compress_default(
            input.as_ptr() as *const c_char,
            output.as_mut_ptr() as *mut c_char,
            (input.len()) as i32,
            output.len() as i32,
        )
    };

    output.truncate(code as usize);

    let compressed_len = code;
    //drop( input );

    b.iter(|| unsafe {
        code = LZ4_decompress_safe(
            output.as_ptr() as *const c_char,
            input.as_mut_ptr() as *mut c_char,
            compressed_len as i32,
            input.len() as i32,
        );
    });
}

#[bench]
fn bench_compress_highratio_1m(b: &mut Bencher) {
    let input = data(1024 * 1024);

    let cz: usize = LZ4_CompressInplaceBufferSize(1024 * 1024);
    let mut output: Vec<u8> = Vec::with_capacity(cz);
    unsafe {
        output.set_len(cz as usize);
    }
    let mut code: i32 = 0;

    b.iter(|| {
        code = unsafe {
            LZ4_compress_default(
                input.as_ptr() as *const c_char,
                output.as_mut_ptr() as *mut c_char,
                (input.len()) as i32,
                output.len() as i32,
            )
        };
    });
}

#[bench]
fn bench_decompress_highratio_1m(b: &mut Bencher) {
    let mut input = data(1024 * 1024);

    let cz: usize = LZ4_CompressInplaceBufferSize(1024 * 1024);
    let mut output: Vec<u8> = Vec::with_capacity(cz);

    unsafe {
        output.set_len(cz as usize);
    }

    let mut code = unsafe {
        LZ4_compress_default(
            input.as_ptr() as *const c_char,
            output.as_mut_ptr() as *mut c_char,
            (input.len()) as i32,
            output.len() as i32,
        )
    };

    output.truncate(code as usize);

    let compressed_len = code;
    //drop( input );

    b.iter(|| unsafe {
        code = LZ4_decompress_safe(
            output.as_ptr() as *const c_char,
            input.as_mut_ptr() as *mut c_char,
            compressed_len as i32,
            input.len() as i32,
        );
    });
}
