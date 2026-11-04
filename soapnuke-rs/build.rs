fn main() {
    let root = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let libdir = out.join("libdeflate");
    let cmake = std::env::var("CMAKE").unwrap_or_else(|_| "cmake".into());
    let cc = std::env::var("CC").unwrap_or_else(|_| "cc".into());
    let ar = std::env::var("AR").unwrap_or_else(|_| "ar".into());
    let mut configure = std::process::Command::new(&cmake);
    configure
        .arg("-S")
        .arg(std::path::Path::new(&root).join("third_party/libdeflate"))
        .arg("-B")
        .arg(&libdir)
        .arg("-DCMAKE_BUILD_TYPE=Release")
        .arg("-DCMAKE_C_FLAGS_RELEASE=-O3 -DNDEBUG")
        .arg("-DCMAKE_POSITION_INDEPENDENT_CODE=ON")
        .arg(format!("-DCMAKE_C_COMPILER={cc}"))
        .arg("-DLIBDEFLATE_BUILD_STATIC_LIB=ON")
        .arg("-DLIBDEFLATE_BUILD_SHARED_LIB=OFF")
        .arg("-DLIBDEFLATE_BUILD_GZIP=OFF")
        .arg("-DLIBDEFLATE_BUILD_TESTS=OFF")
        .arg("-DLIBDEFLATE_INSTALL=OFF");
    if std::env::var_os("AR").is_some() {
        configure.arg(format!("-DCMAKE_AR={ar}"));
    }
    let configured = configure
        .status()
        .expect("cannot start CMake for vendored libdeflate");
    assert!(
        configured.success(),
        "vendored libdeflate configuration failed"
    );
    let built = std::process::Command::new(&cmake)
        .arg("--build")
        .arg(&libdir)
        .arg("--target")
        .arg("libdeflate_static")
        .arg("--parallel")
        .arg("2")
        .status()
        .expect("cannot start vendored libdeflate build");
    assert!(built.success(), "vendored libdeflate compilation failed");
    println!("cargo:rustc-link-search=native={}", libdir.display());
    println!("cargo:rustc-link-lib=static=deflate");
    println!("cargo:rustc-link-lib=dylib=z");
    for name in ["CC", "CXX", "AR", "CMAKE"] {
        println!("cargo:rerun-if-env-changed={name}");
    }
    println!("cargo:rerun-if-changed=third_party/libdeflate");
    println!("cargo:rerun-if-changed=src/rapidgzip_adapter.cpp");
    println!("cargo:rerun-if-changed=src/rapidgzip_adapter.h");
    println!("cargo:rerun-if-changed=third_party/rapidgzip");

    let rapid = std::path::Path::new(&root).join("third_party/rapidgzip");
    assert!(rapid.join("src/rapidgzip/ParallelGzipReader.hpp").is_file());
    assert!(rapid
        .join("librapidarchive/src/filereader/Standard.hpp")
        .is_file());
    let object = out.join("rapidgzip_adapter.o");
    let archive = out.join("libsoapnuke_rapidgzip.a");
    let cxx = std::env::var("CXX").unwrap_or_else(|_| "c++".to_string());
    let compiled = std::process::Command::new(cxx)
        .arg("-std=c++17")
        .arg("-O3")
        .arg("-fPIC")
        .arg("-pthread")
        .arg("-I")
        .arg(rapid.join("src"))
        .arg("-I")
        .arg(rapid.join("librapidarchive/src"))
        .arg("-c")
        .arg(std::path::Path::new(&root).join("src/rapidgzip_adapter.cpp"))
        .arg("-o")
        .arg(&object)
        .status()
        .expect("cannot start C++17 compiler");
    assert!(compiled.success(), "rapidgzip adapter compilation failed");
    let archived = std::process::Command::new(&ar)
        .arg("crus")
        .arg(&archive)
        .arg(&object)
        .status()
        .expect("cannot start ar");
    assert!(archived.success(), "rapidgzip adapter archiving failed");
    println!("cargo:rustc-link-search=native={}", out.display());
    println!("cargo:rustc-link-lib=static=soapnuke_rapidgzip");
    println!("cargo:rustc-link-lib=dylib=stdc++");
    println!("cargo:rustc-link-lib=dylib=pthread");
}
