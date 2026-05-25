// Placeholder TU for the aggregate `celeborn_client` SHARED target.
// All real code is brought in via -Wl,-force_load / --whole-archive of the
// internal static libraries (celeborn_ffi / client / protocol / network /
// proto / memory / conf / utils). CMake still requires at least one source
// file to create the shared library target.
namespace celeborn_ffi {
extern "C" const char* celeborn_client_build_tag() {
  return "celeborn-client-shared";
}
}  // namespace celeborn_ffi
