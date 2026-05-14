use std::path::{Path, PathBuf};
use std::process::Command;

/// Resolve the Celeborn C++ install prefix.
///
/// - If the environment variable `CELEBORN_CPP_PREFIX` is set, use that path
///   directly (pre-built library workflow).
/// - Otherwise, locate the in-repo `cpp/` source tree relative to this crate
///   and drive a full cmake configure → build → install cycle, installing into
///   `$OUT_DIR/celeborn-cpp-install`.  This mirrors the approach used by
///   `openssl-sys` / `libz-sys` and friends.
fn resolve_cpp_prefix() -> PathBuf {
    if let Ok(prefix) = std::env::var("CELEBORN_CPP_PREFIX") {
        let path = PathBuf::from(&prefix);
        if !path.exists() {
            panic!(
                "CELEBORN_CPP_PREFIX is set to '{prefix}' but the directory does not exist. \
                 Please check the path."
            );
        }
        eprintln!("cargo:warning=Using pre-built Celeborn C++ libraries from {prefix}");
        return path;
    }

    // Fall back to building from the in-repo cpp/ source tree.
    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let cpp_source_dir = manifest_dir.join("../../cpp").canonicalize().unwrap_or_else(|_| {
        panic!(
            "CELEBORN_CPP_PREFIX is not set and the in-repo cpp/ directory \
             could not be found relative to {manifest_dir:?}. \
             Either set CELEBORN_CPP_PREFIX or run from within the Celeborn \
             source tree."
        );
    });

    eprintln!(
        "cargo:warning=CELEBORN_CPP_PREFIX not set – building Celeborn C++ \
         from source at {}",
        cpp_source_dir.display()
    );

    cmake_build_cpp(&cpp_source_dir)
}

/// Run cmake configure + build + install for the Celeborn C++ project.
///
/// Returns the install prefix (i.e. the directory that contains `include/` and
/// `lib/`).
fn cmake_build_cpp(source_dir: &Path) -> PathBuf {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let build_dir = out_dir.join("celeborn-cpp-build");
    let install_dir = out_dir.join("celeborn-cpp-install");

    std::fs::create_dir_all(&build_dir).expect("failed to create cmake build directory");
    std::fs::create_dir_all(&install_dir).expect("failed to create cmake install directory");

    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();

    // ── cmake configure ────────────────────────────────────────────────
    let mut configure_cmd = Command::new("cmake");
    configure_cmd
        .current_dir(&build_dir)
        .arg(source_dir)
        .arg(format!("-DCMAKE_INSTALL_PREFIX={}", install_dir.display()))
        .arg("-DCMAKE_BUILD_TYPE=Release")
        .arg("-DCELEBORN_BUILD_TESTS=OFF");

    // On macOS, help cmake find Homebrew-installed dependencies.
    // We pass CMAKE_PREFIX_PATH as a single path to avoid a known issue in the
    // upstream CMakeLists.txt where `if(EXISTS ${CMAKE_PREFIX_PATH}/lib64)`
    // breaks when CMAKE_PREFIX_PATH contains multiple semicolon-separated
    // entries.  Homebrew's default prefix is already on cmake's search path,
    // so we only need to add the keg-only OpenSSL prefix explicitly.
    if target_os == "macos" {
        let homebrew_prefix = std::env::var("HOMEBREW_PREFIX")
            .unwrap_or_else(|_| "/opt/homebrew".to_string());
        configure_cmd.arg(format!(
            "-DCMAKE_PREFIX_PATH={homebrew_prefix}"
        ));
        // OpenSSL is keg-only on Homebrew.  The upstream CMakeLists.txt
        // overwrites the cmake-level OPENSSL_ROOT_DIR variable with a
        // `set()` call, so passing `-DOPENSSL_ROOT_DIR` is not enough.
        // Instead we set the *environment* variable which FindOpenSSL
        // also inspects, and which cannot be shadowed by a cmake `set()`.
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

    // ── cmake build ────────────────────────────────────────────────────
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

    // ── cmake install ──────────────────────────────────────────────────
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

/// Best-effort CPU count for parallel builds (mirrors `cargo`'s own heuristic).
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn main() {
    let prefix = resolve_cpp_prefix();
    let prefix = prefix.display();

    let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();

    let mut builder = cxx_build::bridge("src/lib.rs");
    builder
        .file("src/wrapper.cc")
        .include(format!("{prefix}/include"))
        .include("include")
        .flag_if_supported("-std=c++17");

    // Include paths for system dependencies (folly, boost, etc.)
    if target_os == "macos" {
        builder.include("/opt/homebrew/include");
        builder.include("/opt/homebrew/opt/openssl@3/include");
    }

    if target_arch == "x86_64" {
        builder.flag_if_supported("-msse4.2");
    }

    builder.compile("celeborn_ffi_wrapper");

    // Link cpp/ static libraries (order matters: dependents first)
    println!("cargo:rustc-link-search=native={prefix}/lib");
    for lib in &["client", "protocol", "network", "proto", "memory", "conf", "utils"] {
        println!("cargo:rustc-link-lib=static={lib}");
    }

    // System dependencies
    let base_dylibs = [
        "folly", "glog", "gflags", "protobuf", "fizz", "wangle",
        "ssl", "crypto", "sodium", "lz4", "zstd", "z",
        "fmt", "xxhash", "re2",
        "double-conversion", "event",
    ];
    for lib in &base_dylibs {
        println!("cargo:rustc-link-lib=dylib={lib}");
    }

    // abseil libraries required by protobuf v28+
    let abseil_libs = [
        "absl_log_internal_check_op",
        "absl_log_internal_conditions",
        "absl_log_internal_message",
        "absl_log_internal_nullguard",
        "absl_log_internal_format",
        "absl_log_internal_globals",
        "absl_log_internal_log_sink_set",
        "absl_log_internal_proto",
        "absl_log_internal_fnmatch",
        "absl_log_entry",
        "absl_log_globals",
        "absl_log_initialize",
        "absl_log_severity",
        "absl_log_sink",
        "absl_raw_logging_internal",
        "absl_hash",
        "absl_low_level_hash",
        "absl_city",
        "absl_raw_hash_set",
        "absl_hashtablez_sampler",
        "absl_status",
        "absl_statusor",
        "absl_strings",
        "absl_strings_internal",
        "absl_string_view",
        "absl_str_format_internal",
        "absl_base",
        "absl_spinlock_wait",
        "absl_int128",
        "absl_throw_delegate",
        "absl_time",
        "absl_time_zone",
        "absl_civil_time",
        "absl_synchronization",
        "absl_stacktrace",
        "absl_symbolize",
        "absl_debugging_internal",
        "absl_demangle_internal",
        "absl_malloc_internal",
        "absl_exponential_biased",
        "absl_strerror",
        "absl_cord",
        "absl_cord_internal",
        "absl_cordz_functions",
        "absl_cordz_handle",
        "absl_cordz_info",
        "absl_cordz_sample_token",
        "absl_crc32c",
        "absl_crc_cord_state",
        "absl_crc_cpu_detect",
        "absl_crc_internal",
        "absl_die_if_null",
        "absl_examine_stack",
        "absl_vlog_config_internal",
        "absl_kernel_timeout_internal",
    ];
    for lib in &abseil_libs {
        println!("cargo:rustc-link-lib=dylib={lib}");
    }

    // Boost libraries: macOS Homebrew uses -mt suffix for some components
    let boost_components = [
        "context", "system", "filesystem", "thread",
        "regex", "atomic", "date_time", "program_options",
    ];
    if target_os == "macos" {
        for comp in &boost_components {
            // Try -mt variant first (macOS Homebrew convention)
            println!("cargo:rustc-link-lib=dylib=boost_{comp}-mt");
        }
    } else {
        for comp in &boost_components {
            println!("cargo:rustc-link-lib=dylib=boost_{comp}");
        }
    }

    if target_os == "linux" {
        println!("cargo:rustc-link-lib=dylib=unwind");
        println!("cargo:rustc-link-lib=dylib=stdc++");
    } else if target_os == "macos" {
        println!("cargo:rustc-link-search=native=/opt/homebrew/lib");
        println!("cargo:rustc-link-search=native=/opt/homebrew/opt/openssl@3/lib");
        println!("cargo:rustc-link-lib=dylib=c++");
    } else {
        panic!(
            "unsupported target_os: {target_os} \
             (first version only supports linux/macos)"
        );
    }

    println!("cargo:rerun-if-changed=src/wrapper.cc");
    println!("cargo:rerun-if-changed=include/wrapper.h");
    println!("cargo:rerun-if-env-changed=CELEBORN_CPP_PREFIX");
}
