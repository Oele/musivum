// Ported from LiveKit, pkg/service/roomallocator_test.go at v1.13.11.
// Copyright 2023 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust. There is no router and no local node to make: the
// allocator takes neither. In "return the request with the room preset applied" only the
// half without a preset is ported: named room configurations are left out (see
// `apply_named_room_configuration`), and `require.Same` is equality, since the request comes
// back as a copy. `SelectRoomNode(t)` of the original is not a test (its name does not start
// with `Test`, so Go never runs it); it needs node statistics, which musivum does not
// collect, and is not ported.

use std::sync::Arc;

use super::*;
use crate::config::Config;
use crate::protocol::livekit;
use crate::service::{FakeObjectStore, ObjectStore, RoomAllocator};

#[test]
fn test_create_room() {
    {
        // ensure default room settings are applied
        let conf = Arc::new(Config::new("", true).unwrap());

        let (ra, conf) = new_test_room_allocator(conf);

        let (room, _, _, _) = ra
            .create_room(
                &livekit::CreateRoomRequest {
                    name: "myroom".into(),
                    ..Default::default()
                },
                true,
            )
            .unwrap();
        assert_eq!(conf.room.empty_timeout, room.empty_timeout);
        assert_eq!(conf.room.departure_timeout, room.departure_timeout);
        assert!(!room.enabled_codecs.is_empty());
    }

    {
        // return the request with the room preset applied
        let conf = Arc::new(Config::new("", true).unwrap());

        let (ra, _conf) = new_test_room_allocator(conf);

        // without a preset, the request itself is returned
        let req = livekit::CreateRoomRequest {
            name: "myroom".into(),
            ..Default::default()
        };
        let (_, _, applied, _) = ra.create_room(&req, true).unwrap();
        assert_eq!(req, applied);

        // with a preset, a copy with the preset applied is returned, leaving both the
        // request and the preset unchanged: not ported, there are no presets
    }
}

fn new_test_room_allocator(conf: Arc<Config>) -> (Arc<dyn RoomAllocator>, Arc<Config>) {
    let store = Arc::new(FakeObjectStore::default());
    store.load_room_returns(Err(Error::RoomNotFound));

    let ra = new_room_allocator(conf.clone(), store as Arc<dyn ObjectStore>);
    (ra, conf)
}
