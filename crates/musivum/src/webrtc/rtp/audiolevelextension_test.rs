// Ported from pion/rtp, audiolevelextension_test.go at v1.10.5.
// Copyright (c) The Pion community <https://pion.ly>. Licensed under the MIT License.
// Changes: translated from Go to Rust. The two benchmarks are left out.

use super::*;

#[test]
fn test_audio_level_extension_too_small() {
    let mut a = AudioLevelExtension::default();
    let raw_data: &[u8] = &[];
    assert_eq!(Err(Error::TooSmall), a.unmarshal(raw_data));
}

#[test]
fn test_audio_level_extension_voice_true() {
    let mut a1 = AudioLevelExtension::default();
    let raw_data: &[u8] = &[0x88];
    assert_eq!(Ok(()), a1.unmarshal(raw_data));

    let a2 = AudioLevelExtension {
        level: 8,
        voice: true,
    };
    assert_eq!(a2, a1);

    let dst_data = a2.marshal().unwrap_or_default();
    assert_eq!(raw_data, dst_data);
}

#[test]
fn test_audio_level_extension_voice_false() {
    let mut a1 = AudioLevelExtension::default();
    let raw_data: &[u8] = &[0x8];
    assert_eq!(Ok(()), a1.unmarshal(raw_data));

    let a2 = AudioLevelExtension {
        level: 8,
        voice: false,
    };
    assert_eq!(a2, a1);

    let dst_data = a2.marshal().unwrap_or_default();
    assert_eq!(raw_data, dst_data);
}

#[test]
fn test_audio_level_extension_level_overflow() {
    let a = AudioLevelExtension {
        level: 128,
        voice: false,
    };

    let err = a.marshal();
    assert_eq!(Err(Error::AudioLevelOverflow), err);

    let err = a.marshal_to(&mut [0u8; 10]);
    assert_eq!(Err(Error::AudioLevelOverflow), err);
}

#[test]
fn test_audio_level_extension_marshal_to() {
    let a = AudioLevelExtension {
        level: 8,
        voice: true,
    };

    let mut buf = vec![0u8; a.marshal_size()];
    let n = a.marshal_to(&mut buf);
    assert_eq!(Ok(a.marshal_size()), n);

    let expected = a.marshal().unwrap_or_default();
    assert_eq!(expected, buf);

    let err = a.marshal_to(&mut []);
    assert_eq!(Err(Error::ShortBuffer), err);
}
