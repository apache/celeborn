# Prebuilt Celeborn native artifacts

`celeborn-client-sys/build.rs` looks up the aggregated dylib here before
falling back to a from-source `cmake` build, so dropping the right file
in is all it takes for `cargo build` to work without any environment
variable.

## Layout

```
resource/lib/
  <target-triple>/
    libceleborn_client.dylib    # macOS
    libceleborn_client.so       # Linux
```

`<target-triple>` is the Cargo target identifier — the same string Cargo
prints from `rustc -vV` (`host:` line) or accepts in `cargo build --target`:

| Platform         | Triple                       | Filename                  |
|------------------|------------------------------|---------------------------|
| macOS Apple Silicon | `aarch64-apple-darwin`    | `libceleborn_client.dylib`|
| macOS Intel      | `x86_64-apple-darwin`        | `libceleborn_client.dylib`|
| Linux x86_64     | `x86_64-unknown-linux-gnu`   | `libceleborn_client.so`   |
| Linux aarch64    | `aarch64-unknown-linux-gnu`  | `libceleborn_client.so`   |

## How to refresh the artifact

Build `cpp/` once, then copy the install output:

```bash
cd cpp && mkdir -p build && cd build
OPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3 cmake .. \
  -DCMAKE_BUILD_TYPE=Release -DCELEBORN_BUILD_TESTS=OFF \
  -DCMAKE_PREFIX_PATH=/opt/homebrew \
  -DCMAKE_INSTALL_PREFIX=$PWD/_install
cmake --build . --parallel && cmake --install .

TRIPLE=$(rustc -vV | sed -n 's|host: ||p')
mkdir -p ../../rust/resource/lib/$TRIPLE
cp _install/lib/libceleborn_client.* ../../rust/resource/lib/$TRIPLE/
```

After that, `cargo build` from `rust/` finds the dylib without
`CELEBORN_CPP_PREFIX`.

## Lookup precedence (in `celeborn-client-sys/build.rs`)

1. `CELEBORN_CPP_PREFIX` env var → `<prefix>/lib/libceleborn_client.*`
2. `rust/resource/lib/<target-triple>/libceleborn_client.*` (this directory)
3. From-source `cmake` build of `cpp/` into `$OUT_DIR`
