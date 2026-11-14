// Ported from pion/turn, addr_family.go at v5.0.13.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust.

use super::internal::proto;

// RequestedAddressFamily represents the REQUESTED-ADDRESS-FAMILY Attribute as
// defined in RFC 6156 Section 4.1.1.
pub type RequestedAddressFamily = proto::RequestedAddressFamily;

// Values for RequestedAddressFamily as defined in RFC 6156 Section 4.1.1.
pub const REQUESTED_ADDRESS_FAMILY_IPV4: RequestedAddressFamily = proto::REQUESTED_FAMILY_IPV4;
pub const REQUESTED_ADDRESS_FAMILY_IPV6: RequestedAddressFamily = proto::REQUESTED_FAMILY_IPV6;
