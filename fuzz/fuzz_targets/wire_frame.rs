#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use simraft_node::runtime::FrameBuffer;
use simraft_wire::{
    MAX_FRAME_LEN, decode_config_bytes, decode_entry_bytes, decode_frame, encode_config_bytes,
    encode_entry_bytes, encode_frame, frame_len, inspect_frame_header,
};

fuzz_target!(|data: &[u8]| {
    let header_check = inspect_frame_header(data);
    let len_check = frame_len(data);
    if let Some(Ok(expected_len)) = header_check {
        assert!(expected_len <= MAX_FRAME_LEN);
        assert_eq!(len_check, Some(expected_len));
    } else {
        assert_eq!(len_check, None);
    }

    if let Ok((from, to, msg)) = decode_frame(data) {
        let reencoded = encode_frame(from, to, &msg).expect("decoded frame must re-encode");
        assert!(reencoded.len() <= MAX_FRAME_LEN);
        let round_trip = decode_frame(&reencoded).expect("re-encoded frame must decode");
        assert_eq!(round_trip, (from, to, msg));
    }

    if let Ok(entry) = decode_entry_bytes(data) {
        let reencoded = encode_entry_bytes(&entry).expect("decoded entry must re-encode");
        let round_trip = decode_entry_bytes(&reencoded).expect("re-encoded entry must decode");
        assert_eq!(round_trip, entry);
    }

    if let Ok(cfg) = decode_config_bytes(data) {
        let reencoded = encode_config_bytes(&cfg).expect("decoded config must re-encode");
        let round_trip = decode_config_bytes(&reencoded).expect("re-encoded config must decode");
        assert_eq!(round_trip, cfg);
    }

    let mut whole_buf = FrameBuffer::new();
    whole_buf.push(data);
    while let Some(res) = whole_buf.next_frame() {
        if res.is_err() {
            assert!(whole_buf.is_empty());
            break;
        }
    }

    if !data.is_empty() {
        let split = usize::from(data[0]) % (data.len() + 1);
        let mut split_buf = FrameBuffer::new();
        split_buf.push(&data[..split]);
        let _ = split_buf.next_frame();
        split_buf.push(&data[split..]);
        while let Some(res) = split_buf.next_frame() {
            if res.is_err() {
                assert!(split_buf.is_empty());
                break;
            }
        }
    }
});
