#[cfg(feature = "grpc")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = "proto/retrieve.proto";

    println!("cargo:rerun-if-changed={proto_file}");
    println!("cargo:rerun-if-changed=proto");

    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        // Use bytes::Bytes for BodyChunk.data so the server can slice the
        // object-store buffer zero-copy instead of calling .to_vec() per chunk.
        .bytes(".raccoon.retrieve.v1.BodyChunk.data")
        .compile_protos(&[proto_file], &["proto"])?;

    Ok(())
}

#[cfg(not(feature = "grpc"))]
fn main() {}
