//! Canonical payload codec tests (docs/ARCHITECTURE.md section 7.2).

use simraft_core::ClientId;
use simraft_kv::{
    CommandResult, KvError, KvOp, MAX_PAYLOAD_BYTES, decode_command, decode_result, encode_command,
    encode_result,
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
        let encoded = encode_command(ClientId(9), 42, &op).expect("command encodes");
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
        let encoded = encode_result(&result).expect("result encodes");
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
    )
    .expect("command encodes");
    let cut = &encoded[..encoded.len() - 1];
    match decode_command(cut) {
        Err(err @ KvError::Truncated { field, .. }) => {
            assert_eq!(field, "value", "the truncated field must be named");
            let text = err.to_string();
            assert!(
                !text.contains("  "),
                "no indentation gap in error text: {text}"
            );
        }
        other => panic!("expected a truncation error, got {other:?}"),
    }
}

#[test]
fn unknown_tags_are_named() {
    let mut encoded =
        encode_command(ClientId(1), 1, &KvOp::Delete { key: b"k".to_vec() }).expect("encodes");
    encoded[16] = 99;
    match decode_command(&encoded) {
        Err(err @ KvError::UnknownTag { tag: 99, .. }) => {
            assert!(!err.to_string().contains("  "), "clean formatting: {err}");
        }
        other => panic!("expected UnknownTag 99, got {other:?}"),
    }
    match decode_result(&[77]) {
        Err(KvError::UnknownTag { tag: 77, .. }) => {}
        other => panic!("expected UnknownTag 77, got {other:?}"),
    }
}

#[test]
fn trailing_bytes_are_rejected_for_commands_and_results() {
    let mut cmd =
        encode_command(ClientId(3), 7, &KvOp::Delete { key: b"k".to_vec() }).expect("encodes");
    let cmd_len = cmd.len();
    cmd.push(0xAA);
    assert_eq!(
        decode_command(&cmd),
        Err(KvError::TrailingBytes {
            offset: cmd_len,
            extra: 1,
        })
    );

    let mut res = encode_result(&CommandResult::Ack).expect("encodes");
    res.extend_from_slice(&[1, 2]);
    assert_eq!(
        decode_result(&res),
        Err(KvError::TrailingBytes {
            offset: 1,
            extra: 2,
        })
    );
}

#[test]
fn payloads_above_sixteen_mib_are_rejected_without_panicking() {
    let err = encode_command(
        ClientId(1),
        1,
        &KvOp::Put {
            key: b"k".to_vec(),
            value: vec![0_u8; MAX_PAYLOAD_BYTES],
        },
    )
    .expect_err("must reject > 16 MiB");
    match &err {
        KvError::PayloadTooLarge { length, limit } => {
            assert!(*length > MAX_PAYLOAD_BYTES);
            assert_eq!(*limit, MAX_PAYLOAD_BYTES);
        }
        other => panic!("expected PayloadTooLarge, got {other:?}"),
    }
    let text = err.to_string();
    assert!(text.contains(&MAX_PAYLOAD_BYTES.to_string()), "got: {text}");
}
