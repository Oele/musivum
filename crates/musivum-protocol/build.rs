//! Compiles LiveKit's protocol definitions to Rust (prost), with their JSON form (pbjson),
//! which is how webhooks and the JSON flavour of the room API carry them.
//!
//! The definitions are parsed by protox, so that no `protoc` has to be installed.

use std::path::PathBuf;

const FILES: &[&str] = &[
    "livekit_models.proto",
    "livekit_metrics.proto",
    "livekit_rtc.proto",
    "livekit_room.proto",
    "livekit_egress.proto",
    "livekit_ingress.proto",
    "livekit_agent.proto",
    "livekit_agent_dispatch.proto",
    "livekit_webhook.proto",
    "livekit_internal.proto",
    // The statistics a down track and the connection quality scorer report in
    // (`AnalyticsStat`, `StreamType`); it imports the SIP definitions.
    "livekit_analytics.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = PathBuf::from("protobufs");
    println!("cargo:rerun-if-changed=protobufs");

    let descriptors = protox::compile(FILES, [&dir])?;
    let out = PathBuf::from(std::env::var("OUT_DIR")?);

    let mut config = prost_build::Config::new();
    config
        // Well-known types that serialize to JSON the way Protocol Buffers says they do.
        .compile_well_known_types()
        .extern_path(".google.protobuf", "::pbjson_types");
    config.compile_fds(descriptors.clone())?;

    use protox::prost::Message as _;
    pbjson_build::Builder::new()
        .register_descriptors(&descriptors.encode_to_vec())?
        .out_dir(&out)
        .build(&[".livekit"])?;
    Ok(())
}
