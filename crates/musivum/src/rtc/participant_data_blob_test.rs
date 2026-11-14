// Ported from LiveKit, pkg/rtc/participant_data_blob_test.go at v1.13.11.
// Copyright 2026 LiveKit, Inc. Licensed under the Apache License, Version 2.0.
// Changes: translated from Go to Rust; the text form of a key (`String()`) that one test
// compares is the `Debug` text of the key; the goroutines of the last test are threads.

use super::*;

use crate::protocol::logger;

fn new_test_data_blob() -> ParticipantDataBlob {
    ParticipantDataBlob::new(ParticipantDataBlobParams {
        logger: logger::get_logger(),
    })
}

fn generic_key(name: &str) -> livekit::DataBlobKey {
    livekit::DataBlobKey {
        key: Some(livekit::data_blob_key::Key::Generic(name.to_string())),
    }
}

#[test]
fn test_participant_data_blob_add_and_get() {
    let a = new_test_data_blob();

    let key = generic_key("blob-1");
    let contents = b"definition-bytes".to_vec();

    a.add(Arc::new(livekit::DataBlob {
        key: Some(key.clone()),
        contents: contents.clone(),
    }));

    let got = a.get(Some(&key));
    assert!(got.is_some());
    let got = got.unwrap();
    assert_eq!(
        format!("{key:?}"),
        format!("{:?}", got.key.as_ref().unwrap())
    );
    assert_eq!(contents, got.contents);
}

#[test]
fn test_participant_data_blob_add_overwrites() {
    let a = new_test_data_blob();

    let key = generic_key("blob-1");
    a.add(Arc::new(livekit::DataBlob {
        key: Some(key.clone()),
        contents: b"v1".to_vec(),
    }));
    a.add(Arc::new(livekit::DataBlob {
        key: Some(key.clone()),
        contents: b"v2".to_vec(),
    }));

    let got = a.get(Some(&key));
    assert!(got.is_some());
    assert_eq!(b"v2".to_vec(), got.unwrap().contents);

    assert_eq!(1, a.get_all().len());
}

#[test]
fn test_participant_data_blob_distinct_keys() {
    let a = new_test_data_blob();

    let key1 = generic_key("blob-1");
    let key2 = generic_key("blob-2");

    a.add(Arc::new(livekit::DataBlob {
        key: Some(key1.clone()),
        contents: b"c1".to_vec(),
    }));
    a.add(Arc::new(livekit::DataBlob {
        key: Some(key2.clone()),
        contents: b"c2".to_vec(),
    }));

    let got1 = a.get(Some(&key1));
    assert!(got1.is_some());
    assert_eq!(b"c1".to_vec(), got1.unwrap().contents);

    let got2 = a.get(Some(&key2));
    assert!(got2.is_some());
    assert_eq!(b"c2".to_vec(), got2.unwrap().contents);

    assert_eq!(2, a.get_all().len());
}

#[test]
fn test_participant_data_blob_delete() {
    let a = new_test_data_blob();

    let key = generic_key("blob-1");
    a.add(Arc::new(livekit::DataBlob {
        key: Some(key.clone()),
        contents: b"definition".to_vec(),
    }));

    a.delete(Some(&key));
    assert!(a.get(Some(&key)).is_none());
    assert!(a.get_all().is_empty());

    // deleting a non-existent key is a no-op
    a.delete(Some(&key));
    assert!(a.get_all().is_empty());
}

#[test]
fn test_participant_data_blob_nil_key() {
    let a = new_test_data_blob();

    // nil key should be silently ignored, not panic
    a.add(Arc::new(livekit::DataBlob {
        key: None,
        contents: b"definition".to_vec(),
    }));
    assert!(a.get_all().is_empty());

    assert!(a.get(None).is_none());

    a.delete(None);
    assert!(a.get_all().is_empty());
}

#[test]
fn test_participant_data_blob_get_missing() {
    let a = new_test_data_blob();

    assert!(a.get(Some(&generic_key("missing"))).is_none());
}

#[test]
fn test_participant_data_blob_get_all_contents() {
    let a = new_test_data_blob();

    let key1 = generic_key("blob-1");
    let key2 = generic_key("blob-2");

    a.add(Arc::new(livekit::DataBlob {
        key: Some(key1),
        contents: b"def-1".to_vec(),
    }));
    a.add(Arc::new(livekit::DataBlob {
        key: Some(key2),
        contents: b"def-2".to_vec(),
    }));

    let all = a.get_all();
    assert_eq!(2, all.len());
    for db in &all {
        match db.key.as_ref().unwrap().key.as_ref() {
            Some(livekit::data_blob_key::Key::Generic(generic)) => match generic.as_str() {
                "blob-1" => assert_eq!(b"def-1".to_vec(), db.contents),
                "blob-2" => assert_eq!(b"def-2".to_vec(), db.contents),
                _ => panic!("unexpected key: {generic}"),
            },
            _ => panic!("unexpected key type: Generic"),
        }
    }
}

#[test]
fn test_participant_data_blob_concurrent_access() {
    let a = new_test_data_blob();

    const NUM_GOROUTINES: usize = 16;
    const OPS_PER_GOROUTINE: usize = 100;

    std::thread::scope(|s| {
        for g in 0..NUM_GOROUTINES {
            let a = &a;
            s.spawn(move || {
                for i in 0..OPS_PER_GOROUTINE {
                    let key = generic_key(&format!("blob-{}", g % 8));
                    a.add(Arc::new(livekit::DataBlob {
                        key: Some(key.clone()),
                        contents: b"v".to_vec(),
                    }));
                    let _ = a.get(Some(&key));
                    let _ = a.get_all();
                    if i % 3 == 0 {
                        a.delete(Some(&key));
                    }
                }
            });
        }
    });
}
