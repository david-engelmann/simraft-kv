#![no_main]
#![forbid(unsafe_code)]

use std::io::{Cursor, Read};

use libfuzzer_sys::fuzz_target;
use simraft_node::client::{
    MAX_CLIENT_BODY_BYTES, decode_request, decode_response, encode_request, encode_response,
};

fn read_framed_body(cursor: &mut Cursor<&[u8]>) -> Option<Vec<u8>> {
    let mut prefix = [0_u8; 4];
    cursor.read_exact(&mut prefix).ok()?;
    let body_len = u32::from_le_bytes(prefix) as usize;
    if body_len > MAX_CLIENT_BODY_BYTES {
        return None;
    }
    let remaining = cursor
        .get_ref()
        .len()
        .saturating_sub(cursor.position() as usize);
    if body_len > remaining {
        return None;
    }
    let mut body = vec![0_u8; body_len];
    cursor.read_exact(&mut body).ok()?;
    Some(body)
}

fuzz_target!(|data: &[u8]| {
    if let Ok((client_id, seq, op)) = decode_request(data)
        && let Ok(framed) = encode_request(client_id, seq, &op)
    {
        let body_len = u32::from_le_bytes([framed[0], framed[1], framed[2], framed[3]]) as usize;
        assert_eq!(body_len, data.len());
        assert_eq!(&framed[4..], data);
        let round_trip = decode_request(&framed[4..]).expect("re-encoded request must decode");
        assert_eq!(round_trip, (client_id, seq, op));
    }

    if let Ok(response) = decode_response(data) {
        let framed = encode_response(&response);
        let round_trip = decode_response(&framed[4..]).expect("re-encoded response must decode");
        assert_eq!(round_trip, response);
    }

    let mut cursor = Cursor::new(data);
    while let Some(body) = read_framed_body(&mut cursor) {
        let _ = decode_request(&body);
        let _ = decode_response(&body);
    }
});
