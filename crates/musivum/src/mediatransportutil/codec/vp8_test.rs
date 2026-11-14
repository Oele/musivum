//! New for musivum: the original tests a few fields of what `VP8.Unmarshal` reads
//! (helpers_test.go) and nothing of `Marshal`, `MarshalTo` and `ExtractVP8VideoSize`. Every
//! case here was given to the original, at the version the header of vp8.rs names, and what
//! it answered is what is expected.

use crate::mediatransportutil::codec::*;

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap())
        .collect()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

// The fields in the order of the struct, a flag as 0 or 1.
fn fields(v: &Vp8) -> String {
    format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        v.first_byte,
        v.s as u8,
        v.i as u8,
        v.m as u8,
        v.picture_id,
        v.l as u8,
        v.tl0picidx,
        v.t as u8,
        v.tid,
        v.y as u8,
        v.k as u8,
        v.keyidx,
        v.header_size,
        v.is_key_frame as u8
    )
}

#[test]
fn unmarshal_marshal_and_video_size_as_in_the_original() {
    // The payload; the error of unmarshal and the fields after it; what marshal writes for
    // them; the size of the video.
    #[rustfmt::skip]
    let cases = [
        // a key frame with a picture id of 7 bits, 640x360
        ("9080121002009d012a800268010000", "ok", "144,1,1,0,18,0,0,0,0,0,0,0,3,1", "908012", (640, 360)),
        // no extension, 320x180
        ("101002009d012a4001b4000000", "ok", "16,1,0,0,0,0,0,0,0,0,0,0,1,1", "10", (320, 180)),
        // all of the extension, a picture id of 15 bits; the scaling bits are not in the size
        ("90f09267b49f1002009d012a8082684100", "ok", "144,1,1,1,4711,1,180,1,2,0,1,31,6,1", "90f09267b49f", (640, 360)),
        // the P bit is set: no key frame, no size
        ("9080121102009d012a800268010000", "ok", "144,1,1,0,18,0,0,0,0,0,0,0,3,0", "908012", (0, 0)),
        // not the start of a partition
        ("8080121002009d012a800268010000", "ok", "128,0,1,0,18,0,0,0,0,0,0,0,3,0", "808012", (0, 0)),
        // a key frame without the start code; one that ends before the size
        ("9080121002009d002a800268010000", "ok", "144,1,1,0,18,0,0,0,0,0,0,0,3,1", "908012", (0, 0)),
        ("90801210020000", "ok", "144,1,1,0,18,0,0,0,0,0,0,0,3,1", "908012", (0, 0)),
        // T without K, K without T, both
        ("9020c510", "ok", "144,1,0,0,0,0,0,1,3,0,0,0,3,1", "9020c0", (0, 0)),
        ("9010051002009d012a", "ok", "144,1,0,0,0,0,0,0,0,0,1,5,3,1", "901005", (0, 0)),
        ("9030e51002009d012a", "ok", "144,1,0,0,0,0,0,1,3,1,1,5,3,1", "9030e5", (0, 0)),
        // I, L and T; 16x32
        ("90e0921234851002009d012a100020000000", "ok", "144,1,1,1,4626,1,52,1,2,0,0,0,6,1", "90e092123480", (16, 32)),
        // X without anything in the extension: marshal leaves the extension out
        ("8000000000000000", "ok", "128,0,0,0,0,0,0,0,0,0,0,0,2,0", "00", (0, 0)),
        // what does not parse: the fields that were read before the error stay
        ("", "packet is not large enough", "0,0,0,0,0,0,0,0,0,0,0,0,0,0", "00", (0, 0)),
        ("90", "packet is not large enough", "144,1,0,0,0,0,0,0,0,0,0,0,0,0", "10", (0, 0)),
        ("9080", "packet is not large enough", "144,1,1,0,0,0,0,0,0,0,0,0,0,0", "908000", (0, 0)),
        // L without T
        ("90401210", "invalid packet", "144,1,0,0,0,1,0,0,0,0,0,0,0,0", "904000", (0, 0)),
    ];
    for (payload, err, want_fields, marshalled, size) in cases {
        let p = unhex(payload);
        let mut v = Vp8::default();
        let res = v.unmarshal(&p);
        let got_err = match &res {
            Ok(()) => "ok".to_string(),
            Err(err) => err.to_string(),
        };
        assert_eq!(err, got_err, "{payload}");
        assert_eq!(want_fields, fields(&v), "{payload}");
        assert_eq!(marshalled, hex(&v.marshal().unwrap()), "{payload}");
        let sz = extract_vp8_video_size(&v, &p);
        assert_eq!(size, (sz.width, sz.height), "{payload}");
    }
}

#[test]
fn marshal_as_in_the_original() {
    // Every field, with values that do not fit their bits.
    let v = Vp8 {
        first_byte: 0x00,
        s: true,
        i: true,
        m: true,
        picture_id: 300,
        l: true,
        tl0picidx: 0xb4,
        t: true,
        tid: 0xc5,
        y: true,
        k: true,
        keyidx: 0xff,
        header_size: 6,
        ..Default::default()
    };
    assert_eq!("80f0812cb47f", hex(&v.marshal().unwrap()));
    let mut buf = [0xEE; 8];
    assert_eq!(Ok(6), v.marshal_to(&mut buf));
    assert_eq!("80f0812cb47feeee", hex(&buf));

    // A picture id that does not fit 7 bits, without M.
    let v = Vp8 {
        i: true,
        picture_id: 300,
        ..Default::default()
    };
    assert_eq!("80802c", hex(&v.marshal().unwrap()));

    // I and L, the top bit of the picture id is not written. A buffer that is smaller than
    // the size of the header says.
    let v = Vp8 {
        s: true,
        i: true,
        m: true,
        picture_id: 0x812c,
        l: true,
        tl0picidx: 0xb4,
        tid: 0xc5,
        keyidx: 0xff,
        header_size: 6,
        ..Default::default()
    };
    assert_eq!("80c0812cb4", hex(&v.marshal().unwrap()));
    assert_eq!(Err(Error::ShortPacket), v.marshal_to(&mut [0xEE; 3]));

    // A header that says it is larger than the 8 bytes marshal has.
    let v = Vp8 {
        first_byte: 0xff,
        s: true,
        i: true,
        m: true,
        picture_id: 0x9267,
        l: true,
        tl0picidx: 0x77,
        t: true,
        tid: 0x88,
        y: true,
        k: true,
        keyidx: 0xff,
        header_size: 9,
        ..Default::default()
    };
    assert_eq!(Err(Error::ShortPacket), v.marshal());
    assert_eq!(Err(Error::ShortPacket), v.marshal_to(&mut [0xEE; 2]));
}
