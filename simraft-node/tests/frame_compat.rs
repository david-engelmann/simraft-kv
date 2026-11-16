//! A-04 (docs/INVARIANTS.md section 10): the adapter's peer
//! transport carries section 7.1 frames byte for byte. A frame
//! produced by the shared simraft-wire codec, fed through the
//! node's stream reassembly split at any byte boundary,
//! decodes to exactly the message that was encoded.

use simraft_core::{EntryKind, LogEntry, LogIndex, Message, NodeId, Term};
use simraft_node::runtime::FrameBuffer;
use simraft_wire::encode_frame;

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
    let frame = encode_frame(NodeId(1), NodeId(2), &message);
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
    );
    let second = encode_frame(
        NodeId(2),
        NodeId(1),
        &Message::RequestVoteResponse {
            term: Term(1),
            vote_granted: true,
        },
    );
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
