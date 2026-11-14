// Ported from pion/turn, internal/allocation/event_handler.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. Each callback is an `Option` of an `Arc` of a function;
// an address that may be nil in Go is an `Option`.

use std::net::IpAddr;
use std::sync::Arc;

use crate::webrtc::turn::net::Addr;

pub type OnAuthFn =
    Arc<dyn Fn(Option<Addr>, Option<Addr>, &str, &str, &str, &str, bool) + Send + Sync>;
pub type OnAllocationCreatedFn =
    Arc<dyn Fn(Option<Addr>, Option<Addr>, &str, &str, &str, Option<Addr>, i64) + Send + Sync>;
pub type OnAllocationDeletedFn =
    Arc<dyn Fn(Option<Addr>, Option<Addr>, &str, &str, &str) + Send + Sync>;
pub type OnAllocationErrorFn = Arc<dyn Fn(Option<Addr>, Option<Addr>, &str, &str) + Send + Sync>;
pub type OnPermissionFn =
    Arc<dyn Fn(Option<Addr>, Option<Addr>, &str, &str, &str, Option<Addr>, IpAddr) + Send + Sync>;
pub type OnChannelFn = Arc<
    dyn Fn(Option<Addr>, Option<Addr>, &str, &str, &str, Option<Addr>, Addr, u16) + Send + Sync,
>;

// EventHandler is a set of callbacks that the server will call at certain hook points during an
// allocation's lifecycle. All events are reported with the context that identifies the allocation
// triggering the event (source and destination address, protocol, user-id (as parsed and returned
// by the authentication handler from the TURN username) and realm used for authenticating the
// allocation), plus additional callback specific parameters. It is OK to handle only a subset of
// the callbacks.
#[derive(Clone, Default)]
pub struct EventHandler {
    // OnAuth is called after an authentication request has been processed with the TURN method
    // triggering the authentication request (either "Allocate", "Refresh" "CreatePermission",
    // or "ChannelBind"), and the verdict is the authentication result.
    pub on_auth: Option<OnAuthFn>,
    // OnAllocationCreated is called after a new allocation has been made. The relayAddr
    // argument specifies the relay address and requestedPort is the port requested by the
    // client (if any).
    pub on_allocation_created: Option<OnAllocationCreatedFn>,
    // OnAllocationDeleted is called after an allocation has been removed.
    pub on_allocation_deleted: Option<OnAllocationDeletedFn>,
    // OnAllocationError is called when the readloop hdndling an allocation exits with an
    // error with an error message.
    pub on_allocation_error: Option<OnAllocationErrorFn>,
    // OnPermissionCreated is called after a new permission has been made to an IP address.
    pub on_permission_created: Option<OnPermissionFn>,
    // OnPermissionDeleted is called after a permission for a given IP address has been
    // removed.
    pub on_permission_deleted: Option<OnPermissionFn>,
    // OnChannelCreated is called after a new channel has been made. The relay address, the
    // peer address and the channel number can be used to uniquely identify the channel
    // created.
    pub on_channel_created: Option<OnChannelFn>,
    // OnChannelDeleted is called after a channel has been removed from the server. The relay
    // address, the peer address and the channel number can be used to uniquely identify the
    // channel deleted.
    pub on_channel_deleted: Option<OnChannelFn>,
}
