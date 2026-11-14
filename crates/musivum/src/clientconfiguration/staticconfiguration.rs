// Ported from LiveKit, pkg/clientconfiguration/staticconfiguration.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the client's information is a reference, which cannot
// be nil: the original panics on a nil one as soon as a rule reads a field of it. The
// configuration is given by value: of a rule that is not merged it is a copy, where the
// original gives the pointer that is in its table. `proto.Merge` is the function `merge` at
// the end.

use std::sync::Arc;

use musivum_protocol::Message;

use super::{ClientConfigurationManager, Match};
use crate::errorw;
use crate::protocol::livekit;
use crate::protocol::logger;
use crate::utils;

#[derive(Clone)]
pub struct ConfigurationItem {
    pub r#match: Arc<dyn Match>,
    pub configuration: Option<livekit::ClientConfiguration>,
    pub merge: bool,
}

pub struct StaticClientConfigurationManager {
    confs: Vec<ConfigurationItem>,
}

impl StaticClientConfigurationManager {
    pub fn new(confs: Vec<ConfigurationItem>) -> StaticClientConfigurationManager {
        StaticClientConfigurationManager { confs }
    }
}

impl ClientConfigurationManager for StaticClientConfigurationManager {
    fn get_configuration(
        &self,
        client_info: &livekit::ClientInfo,
    ) -> Option<livekit::ClientConfiguration> {
        let mut matched_conf: Vec<Option<&livekit::ClientConfiguration>> = Vec::new();
        for c in &self.confs {
            let matched = match c.r#match.r#match(client_info) {
                Ok(matched) => matched,
                Err(err) => {
                    errorw!(
                        logger::get_logger(),
                        "matchrule failed",
                        err,
                        "clientInfo" => utils::client_info_without_address(Some(client_info)),
                    );
                    continue;
                }
            };
            if !matched {
                continue;
            }
            if !c.merge {
                return c.configuration.clone();
            }
            matched_conf.push(c.configuration.as_ref());
        }

        let mut conf: Option<livekit::ClientConfiguration> = None;
        for (k, v) in matched_conf.iter().enumerate() {
            if k == 0 {
                conf = matched_conf[0].cloned();
            } else {
                // TODO : there is a problem use protobuf merge, we don't have flag to indicate 'no value',
                // don't override default behavior or other configuration's field. So a bool value = false or
                // a int value = 0 will override same field in other configuration
                merge(conf.as_mut(), *v);
            }
        }
        conf
    }
}

// Go's `proto.Merge(dst, src)`: what `src` has set is set in `dst`, its lists are added to
// the lists of `dst`, and a message inside it is merged into the one in `dst` in the same
// way. A number or a setting that is zero in `src` counts as not set, and leaves what `dst`
// has. That is what reading a message's encoding into another message does, by the rules of
// Protocol Buffers, so this does that. Like the original it panics when there is no message
// to merge into (the first rule that matched has no configuration and a later one is merged
// into it), and does nothing when there is none to merge.
fn merge(
    dst: Option<&mut livekit::ClientConfiguration>,
    src: Option<&livekit::ClientConfiguration>,
) {
    let dst = dst.expect("invalid value: merging into nil message");
    if let Some(src) = src {
        dst.merge(src.encode_to_vec().as_slice())
            .expect("a message that was just encoded can be decoded");
    }
}
