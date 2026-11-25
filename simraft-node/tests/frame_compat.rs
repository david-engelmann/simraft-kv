//! A-04 (docs/INVARIANTS.md section 10): the adapter's peer
//! transport carries wire frames byte for byte, reassembling
//! arbitrary splits while rejecting corrupt magic or oversized
//! frame headers (> 16 MiB) immediately without buffering.

use simraft_core::{EntryKind, LogEntry, LogIndex, Message, NodeId, Term};
use simraft_node::runtime::FrameBuffer;
use simraft_wire::{MAX_FRAME_BODY_LEN, WireError, encode_frame};

#[test]
fn frames_reassemble_at_any_split() {
    let message = Message::AppendEntries {
        term: Term(3),
        prev_log_index: LogIndex(4),
        prev_log_term: Term(2),
        entries: vec![LogEntry {
            term: Term(3),
            kind: EntryKind::Command {
                client_id: simraft_core::ClientId(9),
                seq: 5,
                payload: b"a kv command payload".to_vec(),
            },
        }],
        leader_commit: LogIndex(4),
        read_round: 0,
    };
    let frame = encode_frame(NodeId(1), NodeId(2), &message).expect("frame encodes");
    for split in 0..=frame.len() {
        let mut buffer = FrameBuffer::new();
        buffer.push(&frame[..split]);
        if split < frame.len() {
            assert!(
                buffer.next_frame().is_none(),
                "no frame is complete at split {split}"
            );
        }
        buffer.push(&frame[split..]);
        let (from, to, decoded) = buffer
            .next_frame()
            .expect("the frame completes")
            .expect("the frame decodes");
        assert_eq!(from, NodeId(1), "split {split}: the sender survives");
        assert_eq!(to, NodeId(2), "split {split}: the recipient survives");
        assert_eq!(decoded, message, "split {split}: the message is identical");
        assert!(buffer.next_frame().is_none(), "the buffer is drained");
    }
}

#[test]
fn back_to_back_frames_come_out_in_order() {
    let first = encode_frame(
        NodeId(2),
        NodeId(1),
        &Message::RequestVote {
            term: Term(1),
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
        },
    )
    .expect("first encodes");
    let second = encode_frame(
        NodeId(2),
        NodeId(1),
        &Message::RequestVoteResponse {
            term: Term(1),
            vote_granted: true,
        },
    )
    .expect("second encodes");
    let mut buffer = FrameBuffer::new();
    let mut glued = first;
    glued.extend_from_slice(&second);
    buffer.push(&glued);
    let (_, _, one) = buffer
        .next_frame()
        .expect("the first frame")
        .expect("the first decodes");
    let (_, _, two) = buffer
        .next_frame()
        .expect("the second frame")
        .expect("the second decodes");
    assert!(matches!(one, Message::RequestVote { .. }));
    assert!(matches!(
        two,
        Message::RequestVoteResponse {
            vote_granted: true,
            ..
        }
    ));
}

#[test]
fn frame_buffer_rejects_bad_magic_and_four_gigabyte_header_immediately() {
    let mut bad_magic = FrameBuffer::new();
    bad_magic.push(b"NOPE");
    assert!(matches!(
        bad_magic.next_frame(),
        Some(Err(WireError::BadMagic { .. }))
    ));
    assert!(bad_magic.is_empty());

    let valid = encode_frame(
        NodeId(1),
        NodeId(2),
        &Message::RequestVote {
            term: Term(1),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::ZERO,
        },
    )
    .expect("encodes");
    let mut header_only = valid[..36].to_vec();
    header_only[32..36].copy_from_slice(&u32::MAX.to_le_bytes());
    let mut huge = FrameBuffer::new();
    huge.push(&header_only);
    assert!(
        matches!(
            huge.next_frame(),
            Some(Err(WireError::PayloadTooLarge {
                len: 0xFFFF_FFFF,
                limit: MAX_FRAME_BODY_LEN,
            }))
        ),
        "FrameBuffer must reject a 4 GiB body_len immediately from the 36-byte header"
    );
    assert!(huge.is_empty());
}
