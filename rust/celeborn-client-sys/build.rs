fn main() {
    let prefix = std::env::var("CELEBORN_CPP_PREFIX")
        .expect("set CELEBORN_CPP_PREFIX to cpp/build/installed first");

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
