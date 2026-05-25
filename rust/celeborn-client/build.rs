// Re-emit the dylib directory exported by `celeborn-client-sys` (via its
// `links = "celeborn_client"` metadata) as an rpath on every artifact this
// crate produces (examples, integration tests, downstream binaries).
//
// `cargo:rustc-link-arg=...` from a sys crate's build script does *not*
// propagate to dependent crates' link lines, so the rpath has to be emitted
// from the crate that owns those artifacts.
fn main() {
    if let Ok(lib_dir) = std::env::var("DEP_CELEBORN_CLIENT_LIB_DIR") {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{lib_dir}");
    }
    println!("cargo:rerun-if-env-changed=DEP_CELEBORN_CLIENT_LIB_DIR");
}
