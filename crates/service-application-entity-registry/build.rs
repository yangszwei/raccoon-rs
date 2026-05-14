#[cfg(feature = "grpc")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = [
        "proto/application_entity_registry.proto",
        "proto/application_entity_registry_writer.proto",
    ];

    for proto_file in proto_files {
        println!("cargo:rerun-if-changed={proto_file}");
    }
    println!("cargo:rerun-if-changed=proto");

    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&proto_files, &["proto"])?;

    Ok(())
}

#[cfg(not(feature = "grpc"))]
fn main() {}
