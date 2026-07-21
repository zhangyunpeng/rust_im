use prost_build::Config;
use std::path::PathBuf;

fn main() -> std::io::Result<()> {
    let mut cfg = Config::new();
    // cfg.enum_attribute(".", "#[derive(Debug, Clone, Copy, PartialEq, Eq)]");
    // cfg.message_attribute(".", "#[derive(Debug, Clone)]");

    let proto_file = PathBuf::from("proto/message.proto");
    let out_dir = PathBuf::from("src/pb/generated");

    std::fs::create_dir_all(&out_dir)?;

    cfg.out_dir(out_dir)
        .compile_protos(&[proto_file], &["proto"])
        .expect("compile proto failed");

    println!("cargo:rerun-if-changed=proto/message.proto");
    Ok(())
}
