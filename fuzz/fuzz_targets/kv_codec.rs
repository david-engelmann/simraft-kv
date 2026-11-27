#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use simraft_kv::{
    KvStateMachine, MAX_PAYLOAD_BYTES, decode_command, decode_result, decode_snapshot,
    encode_command, encode_result, encode_snapshot,
};

fuzz_target!(|data: &[u8]| {
    if let Ok((client_id, seq, op)) = decode_command(data) {
        let reencoded =
            encode_command(client_id, seq, &op).expect("decoded command must re-encode");
        assert!(reencoded.len() <= MAX_PAYLOAD_BYTES);
        assert_eq!(reencoded.as_slice(), data);
        let round_trip = decode_command(&reencoded).expect("re-encoded command must decode");
        assert_eq!(round_trip, (client_id, seq, op));
    }

    if let Ok(result) = decode_result(data) {
        let reencoded = encode_result(&result).expect("decoded result must re-encode");
        assert!(reencoded.len() <= MAX_PAYLOAD_BYTES);
        assert_eq!(reencoded.as_slice(), data);
        let round_trip = decode_result(&reencoded).expect("re-encoded result must decode");
        assert_eq!(round_trip, result);
    }

    if let Ok(parts) = decode_snapshot(data) {
        let reencoded = encode_snapshot(&parts).expect("decoded snapshot must re-encode");
        let round_trip = decode_snapshot(&reencoded).expect("re-encoded snapshot must decode");
        assert_eq!(round_trip, parts);

        let mut machine = KvStateMachine::default();
        machine.install_snapshot(parts);
    }
});
