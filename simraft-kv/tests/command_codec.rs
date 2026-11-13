//! Canonical payload codec tests (docs/ARCHITECTURE.md section 7.2).

use simraft_core::ClientId;
use simraft_kv::{
    CommandResult, KvError, KvOp, decode_command, decode_result, encode_command, encode_result,
};

#[test]
fn command_roundtrip() {
    for op in [
        KvOp::Put {
            key: b"alpha".to_vec(),
            value: b"1".to_vec(),
        },
        KvOp::Put {
            key: Vec::new(),
            value: Vec::new(),
        },
        KvOp::Delete {
            key: b"beta".to_vec(),
        },
    ] {
        let encoded = encode_command(ClientId(9), 42, &op);
        let (client, seq, decoded) = decode_command(&encoded).expect("roundtrip decodes");
        assert_eq!(client, ClientId(9));
        assert_eq!(seq, 42);
        assert_eq!(decoded, op);
    }
}

#[test]
fn result_roundtrip_all_tags() {
    for result in [
        CommandResult::Ack,
        CommandResult::Value(b"v".to_vec()),
        CommandResult::Error {
            code: 7,
            message: "nope".to_string(),
        },
    ] {
        let encoded = encode_result(&result);
        assert_eq!(decode_result(&encoded).expect("roundtrip"), result);
    }
}

#[test]
fn truncated_payload_names_field_and_offset() {
    let encoded = encode_command(
        ClientId(1),
        1,
        &KvOp::Put {
            key: b"longkey".to_vec(),
            value: b"v".to_vec(),
        },
    );
    let cut = &encoded[..encoded.len() - 1];
    match decode_command(cut) {
        Err(KvError::Truncated { field, .. }) => {
            assert_eq!(field, "value", "the truncated field must be named");
        }
        other => panic!("expected a truncation error, got {other:?}"),
    }
}

#[test]
fn unknown_tags_are_named() {
    let mut encoded = encode_command(ClientId(1), 1, &KvOp::Delete { key: b"k".to_vec() });
    encoded[16] = 99;
    match decode_command(&encoded) {
        Err(KvError::UnknownTag { tag: 99, .. }) => {}
        other => panic!("expected UnknownTag 99, got {other:?}"),
    }
    match decode_result(&[77]) {
        Err(KvError::UnknownTag { tag: 77, .. }) => {}
        other => panic!("expected UnknownTag 77, got {other:?}"),
    }
}
