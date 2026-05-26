// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Resolve the directory containing `libceleborn_client.{so,dylib}`.
///
/// Lookup order (first match wins):
/// 1. `CELEBORN_CPP_PREFIX` env var → use `<prefix>/lib`. Intended for CI /
///    custom installs.
/// 2. In-repo prebuilt artifact at
///    `rust/resource/lib/<target-triple>/libceleborn_client.{so,dylib}`.
///    Drop the matching dylib in and `cargo build` picks it up with no
///    extra environment variable.
/// 3. Fall back to driving `cmake` against the in-repo `cpp/` source tree
///    and installing into `$OUT_DIR/celeborn-cpp-install/lib`.
fn resolve_lib_dir() -> PathBuf {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();
    let lib_filename = match target_os.as_str() {
        "macos" => "libceleborn_client.dylib",
        "linux" => "libceleborn_client.so",
        other => panic!("unsupported target_os: {other} (only linux/macos supported)"),
    };

    // 1. Explicit override.
    if let Ok(prefix) = std::env::var("CELEBORN_CPP_PREFIX") {
        let lib_dir = PathBuf::from(&prefix).join("lib");
        if !lib_dir.join(lib_filename).exists() {
            panic!(
                "CELEBORN_CPP_PREFIX={prefix} but {} does not exist.",
                lib_dir.join(lib_filename).display()
            );
        }
        eprintln!("cargo:warning=Using prebuilt Celeborn dylib from {prefix}");
        return lib_dir;
    }

    // 2. In-repo prebuilt: rust/resource/lib/<target>/libceleborn_client.<ext>
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let target = std::env::var("TARGET").unwrap();
    let resource_lib_dir = manifest_dir
        .join("../resource/lib")
        .join(&target);
    if resource_lib_dir.join(lib_filename).exists() {
        eprintln!(
            "cargo:warning=Using in-repo prebuilt Celeborn dylib at {}",
            resource_lib_dir.display()
        );
        return resource_lib_dir
            .canonicalize()
            .expect("failed to canonicalize resource lib dir");
    }

    // 3. Fall back to cmake from source.
    let cpp_source_dir = manifest_dir
        .join("../../cpp")
        .canonicalize()
        .unwrap_or_else(|_| {
            panic!(
                "No prebuilt {lib_filename} found at {}, CELEBORN_CPP_PREFIX is unset, \
                 and the in-repo cpp/ directory is not reachable from {}.",
                resource_lib_dir.display(),
                manifest_dir.display(),
            )
        });

    eprintln!(
        "cargo:warning=No prebuilt dylib at {}; building Celeborn C++ from source at {}",
        resource_lib_dir.display(),
        cpp_source_dir.display()
    );

    cmake_build_cpp(&cpp_source_dir).join("lib")
}

fn cmake_build_cpp(source_dir: &Path) -> PathBuf {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let build_dir = out_dir.join("celeborn-cpp-build");
    let install_dir = out_dir.join("celeborn-cpp-install");

    std::fs::create_dir_all(&build_dir).expect("failed to create cmake build directory");
    std::fs::create_dir_all(&install_dir).expect("failed to create cmake install directory");

    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();

    let mut configure_cmd = Command::new("cmake");
    configure_cmd
        .current_dir(&build_dir)
        .arg(source_dir)
        .arg(format!("-DCMAKE_INSTALL_PREFIX={}", install_dir.display()))
        .arg("-DCMAKE_BUILD_TYPE=Release")
        .arg("-DCELEBORN_BUILD_TESTS=OFF");

    if target_os == "macos" {
        let homebrew_prefix = std::env::var("HOMEBREW_PREFIX")
            .unwrap_or_else(|_| "/opt/homebrew".to_string());
        configure_cmd.arg(format!("-DCMAKE_PREFIX_PATH={homebrew_prefix}"));
        configure_cmd.env(
            "OPENSSL_ROOT_DIR",
            format!("{homebrew_prefix}/opt/openssl@3"),
        );
    }

    let configure_status = configure_cmd
        .status()
        .expect("failed to execute `cmake` – is cmake installed?");
    if !configure_status.success() {
        panic!("cmake configure step failed (exit code: {configure_status})");
    }

    let num_jobs = std::env::var("NUM_JOBS").unwrap_or_else(|_| num_cpus().to_string());
    let build_status = Command::new("cmake")
        .current_dir(&build_dir)
        .args(["--build", "."])
        .args(["--config", "Release"])
        .args(["--parallel", &num_jobs])
        .status()
        .expect("failed to execute cmake --build");
    if !build_status.success() {
        panic!("cmake build step failed (exit code: {build_status})");
    }

    let install_status = Command::new("cmake")
        .current_dir(&build_dir)
        .args(["--install", "."])
        .status()
        .expect("failed to execute cmake --install");
    if !install_status.success() {
        panic!("cmake install step failed (exit code: {install_status})");
    }

    install_dir
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn main() {
    let lib_dir = resolve_lib_dir();
    let lib_dir_str = lib_dir.display().to_string();

    // Single aggregated dylib: libceleborn_client.{so,dylib} bundles every
    // internal static lib (including the celeborn_ffi C ABI shim) and pulls
    // third-party deps via NEEDED entries that resolve at runtime. The Rust
    // crate touches no C++ headers and links nothing else.
    println!("cargo:rustc-link-search=native={lib_dir_str}");
    println!("cargo:rustc-link-lib=dylib=celeborn_client");

    // Re-export lib_dir so the downstream `celeborn-client` crate can pick
    // it up via DEP_CELEBORN_CLIENT_LIB_DIR and embed an rpath in its
    // examples / tests / binaries. (`cargo:rustc-link-arg` does not
    // propagate from a sys crate to dependent crates' artifacts.)
    println!("cargo:lib_dir={lib_dir_str}");

    println!("cargo:rerun-if-env-changed=CELEBORN_CPP_PREFIX");
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    println!("cargo:rerun-if-changed={manifest_dir}/../resource/lib");

    // When we fall back to building cpp/ from source (option 3 in
    // resolve_lib_dir), edits under cpp/ must trigger a rebuild of the
    // dylib. Declare the dependency unconditionally so that toggling
    // between prebuilt and from-source modes does not require touching
    // the build script.
    let cpp_dir = PathBuf::from(&manifest_dir).join("../../cpp");
    if cpp_dir.exists() {
        let cpp_dir_str = cpp_dir.display();
        println!("cargo:rerun-if-changed={cpp_dir_str}/CMakeLists.txt");
        println!("cargo:rerun-if-changed={cpp_dir_str}/celeborn");
    }
}
