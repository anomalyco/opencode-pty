use std::{env, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("windows")
        || env::var("CARGO_CFG_TARGET_ENV").as_deref() != Ok("msvc")
    {
        return;
    }

    // libghostty-vt-sys 0.2.1 links the DLL import library on MSVC instead
    // of the static archive it builds. Keep the workaround until the next
    // release includes https://github.com/Uzaaft/libghostty-rs/commit/bac73b914d936e945de4a6b93bed75ae1ce8895c.
    let include =
        PathBuf::from(env::var_os("DEP_GHOSTTY_VT_INCLUDE").expect("Ghostty include path"));
    let library = include
        .parent()
        .expect("Ghostty install directory")
        .join("lib");
    assert!(library.join("ghostty-vt-static.lib").is_file());
    println!("cargo:rustc-link-search=native={}", library.display());
    println!("cargo:rustc-link-lib=static=ghostty-vt-static");
    println!("cargo:rustc-link-arg=/NODEFAULTLIB:ghostty-vt.lib");
}
