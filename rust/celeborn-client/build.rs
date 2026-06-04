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
