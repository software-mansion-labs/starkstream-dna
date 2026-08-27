use std::{env, io::Result, path::PathBuf, println};

static DNA_STREAM_DESCRIPTOR_FILE: &str = "dna_stream_v2_descriptor.bin";

fn main() -> Result<()> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    println!("cargo:rerun-if-changed=proto");

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .skip_debug(["StreamDataRequest"])
        .bytes(".dna.v2.stream.Data.data")
        .file_descriptor_set_path(out_dir.join(DNA_STREAM_DESCRIPTOR_FILE))
        .compile_protos(&["proto/dna/v2/stream.proto"], &["proto/dna/"])?;

    /*
     * Starknet
     */
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .boxed(".starknet.v2.InvokeTransactionTrace.execute_invocation.success")
        .boxed(".starknet.v2.L1HandlerTransactionTrace.execute_invocation.success")
        .boxed(".starknet.v2.TransactionTrace.trace_root.deploy_account")
        .compile_protos(
            &[
                "proto/starknet/v2/data.proto",
                "proto/starknet/v2/filter.proto",
            ],
            &["proto/starknet/"],
        )?;

    Ok(())
}
