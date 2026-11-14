// Ported from LiveKit's protocol, utils/data_channel_rpc.go at
// v1.53.1-0.20261023175527-46332a036e25.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0. (The original
// has no copyright line of its own; this is the notice of its repository.)
// Changes: translated from Go to Rust; the error code stays a number with named values, not
// an enum, because a client can send any code; the map of messages is a function;
// `PsrpcError()`, the conversion for LiveKit's RPC between nodes, is left out.

use std::fmt;

use crate::go::time::Duration;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Debug)]
pub struct DataChannelRpcErrorCode(pub u32);

pub const DATA_CHANNEL_RPC_APPLICATION_ERROR: DataChannelRpcErrorCode =
    DataChannelRpcErrorCode(1500);
pub const DATA_CHANNEL_RPC_CONNECTION_TIMEOUT: DataChannelRpcErrorCode =
    DataChannelRpcErrorCode(1501);
pub const DATA_CHANNEL_RPC_RESPONSE_TIMEOUT: DataChannelRpcErrorCode =
    DataChannelRpcErrorCode(1502);
pub const DATA_CHANNEL_RPC_RECIPIENT_DISCONNECTED: DataChannelRpcErrorCode =
    DataChannelRpcErrorCode(1503);
pub const DATA_CHANNEL_RPC_RESPONSE_PAYLOAD_TOO_LARGE: DataChannelRpcErrorCode =
    DataChannelRpcErrorCode(1504);
pub const DATA_CHANNEL_RPC_SEND_FAILED: DataChannelRpcErrorCode = DataChannelRpcErrorCode(1505);

pub const DATA_CHANNEL_RPC_UNSUPPORTED_METHOD: DataChannelRpcErrorCode =
    DataChannelRpcErrorCode(1400);
pub const DATA_CHANNEL_RPC_RECIPIENT_NOT_FOUND: DataChannelRpcErrorCode =
    DataChannelRpcErrorCode(1401);
pub const DATA_CHANNEL_RPC_REQUEST_PAYLOAD_TOO_LARGE: DataChannelRpcErrorCode =
    DataChannelRpcErrorCode(1402);
pub const DATA_CHANNEL_RPC_UNSUPPORTED_SERVER: DataChannelRpcErrorCode =
    DataChannelRpcErrorCode(1403);
pub const DATA_CHANNEL_RPC_UNSUPPORTED_VERSION: DataChannelRpcErrorCode =
    DataChannelRpcErrorCode(1404);

pub const DATA_CHANNEL_RPC_MAX_ROUND_TRIP_LATENCY: Duration = Duration::from_millis(7000);
pub const DATA_CHANNEL_RPC_MAX_MESSAGE_BYTES: usize = 256;
pub const DATA_CHANNEL_RPC_MAX_DATA_BYTES: usize = 15360; // 15KiB
pub const DATA_CHANNEL_RPC_MAX_PAYLOAD_BYTES: usize = 15360; // 15KiB
pub const DATA_CHANNEL_RPC_DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_millis(15000);

fn data_channel_rpc_error_messages(code: DataChannelRpcErrorCode) -> &'static str {
    match code {
        DATA_CHANNEL_RPC_APPLICATION_ERROR => "Application error in method handler",
        DATA_CHANNEL_RPC_CONNECTION_TIMEOUT => "Connection timeout",
        DATA_CHANNEL_RPC_RESPONSE_TIMEOUT => "Response timeout",
        DATA_CHANNEL_RPC_RECIPIENT_DISCONNECTED => "Recipient disconnected",
        DATA_CHANNEL_RPC_RESPONSE_PAYLOAD_TOO_LARGE => "Response payload too large",
        DATA_CHANNEL_RPC_SEND_FAILED => "Failed to send",

        DATA_CHANNEL_RPC_UNSUPPORTED_METHOD => "Method not supported at destination",
        DATA_CHANNEL_RPC_RECIPIENT_NOT_FOUND => "Recipient not found",
        DATA_CHANNEL_RPC_REQUEST_PAYLOAD_TOO_LARGE => "Request payload too large",
        DATA_CHANNEL_RPC_UNSUPPORTED_SERVER => "RPC not supported by server",
        DATA_CHANNEL_RPC_UNSUPPORTED_VERSION => "Unsupported RPC version",

        _ => "",
    }
}

#[derive(Clone, PartialEq, Eq, Default, Debug)]
pub struct DataChannelRpcError {
    pub code: DataChannelRpcErrorCode,
    pub message: String,
    pub data: String,
}

impl fmt::Display for DataChannelRpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.data.is_empty() {
            return write!(
                f,
                "RpcError {}: {}: {}",
                self.code.0, self.message, self.data
            );
        }
        write!(f, "RpcError {}: {}", self.code.0, self.message)
    }
}

impl std::error::Error for DataChannelRpcError {}

pub fn data_channel_rpc_error_from_built_in_codes(
    code: DataChannelRpcErrorCode,
    data: &str,
) -> DataChannelRpcError {
    DataChannelRpcError {
        code,
        message: data_channel_rpc_error_messages(code).to_string(),
        data: data.to_string(),
    }
}

pub struct DataChannelRpcPendingAckHandler {
    pub resolve: Box<dyn Fn() + Send + Sync>,
    pub participant_identity: String,
}

#[allow(clippy::type_complexity)]
pub struct DataChannelRpcPendingResponseHandler {
    pub resolve: Box<dyn Fn(&str, Option<DataChannelRpcError>) + Send + Sync>,
    pub participant_identity: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    // New for musivum: upstream has no test for this file.
    #[test]
    fn built_in_codes_have_their_messages() {
        let err =
            data_channel_rpc_error_from_built_in_codes(DATA_CHANNEL_RPC_RECIPIENT_DISCONNECTED, "");
        assert_eq!(DataChannelRpcErrorCode(1503), err.code);
        assert_eq!("RpcError 1503: Recipient disconnected", err.to_string());

        let err = data_channel_rpc_error_from_built_in_codes(
            DATA_CHANNEL_RPC_REQUEST_PAYLOAD_TOO_LARGE,
            "16000 bytes",
        );
        assert_eq!(
            "RpcError 1402: Request payload too large: 16000 bytes",
            err.to_string()
        );

        // a code that is not built in has no message
        let err = data_channel_rpc_error_from_built_in_codes(DataChannelRpcErrorCode(42), "");
        assert_eq!("RpcError 42: ", err.to_string());

        assert_eq!(7000, DATA_CHANNEL_RPC_MAX_ROUND_TRIP_LATENCY.milliseconds());
        assert_eq!(
            15000,
            DATA_CHANNEL_RPC_DEFAULT_RESPONSE_TIMEOUT.milliseconds()
        );
    }
}
