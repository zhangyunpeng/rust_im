use std::path::PathBuf;

fn main() -> std::io::Result<()> {
    let out_dir = PathBuf::from("src/pb/generated");
    std::fs::create_dir_all(&out_dir)?;

    tonic_prost_build::configure()
        .out_dir(out_dir)
        // 正确方法名：build_transport，不是 transport
        .build_transport(true)
        // 0.14.6 保留 compile_protos，不要用 compile
        .compile_protos(&["proto/message.proto", "proto/comet.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/message.proto");
    println!("cargo:rerun-if-changed=proto/comet.proto");
    Ok(())
}
