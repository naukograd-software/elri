//! Compiles the protocol definitions into a descriptor set, comments
//! included, for the agent's generic access to the core API.

// A build script fails loudly; that is what it is for.
#![allow(clippy::expect_used)]

fn main() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../protocols");
    let mut files = Vec::new();
    collect(&root.join("elri"), &mut files);
    files.sort();
    println!("cargo:rerun-if-changed={}", root.display());
    let rel: Vec<String> = files
        .iter()
        .map(|f| {
            f.strip_prefix(&root)
                .expect("under protocols")
                .to_string_lossy()
                .replace('\\', "/")
        })
        .collect();
    let set = protox::Compiler::new([&root])
        .expect("compiler")
        .include_source_info(true)
        .include_imports(true)
        .open_files(&rel)
        .expect("protocols compile")
        .encode_file_descriptor_set();
    let out = std::path::Path::new(&std::env::var("OUT_DIR").expect("OUT_DIR")).join("api.bin");
    std::fs::write(out, set).expect("write descriptors");
}

fn collect(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    for e in std::fs::read_dir(dir).expect("read protocols") {
        let p = e.expect("entry").path();
        if p.is_dir() {
            collect(&p, out);
        } else if p.extension().is_some_and(|x| x == "proto") {
            out.push(p);
        }
    }
}
