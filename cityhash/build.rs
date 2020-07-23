extern crate cc;

fn main() {
    let mut compiler = cc::Build::new();
    compiler.file("src/cc/city.cc").cpp(true).opt_level(3);
    // if cfg!(target_feature  = "sse4.2") {
    //     compiler.define("__SSE4_2__", "1");
    // }
    compiler.compile("libcityhash");
}
