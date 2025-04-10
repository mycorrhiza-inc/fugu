fn main() {
    // Compile protobuf files
    tonic_build::compile_protos("proto/namespace.proto")
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));

    // Tell Cargo to rerun this script if the proto files change
    println!("cargo:rerun-if-changed=proto/namespace.proto");
    println!("cargo:rerun-if-changed=proto/helloworld.proto");
    println!("cargo:rerun-if-changed=proto/system.proto");
}
