//! The node runtime: threads, sockets, and the driver loop.
//!
//! One driver thread owns the core node, the KV state machine,
//! and the file store, and steps the core for every input, the
//! same discipline the simulator's harness uses. Around it: a
//! ticker thread feeding `Tick` events at the configured
//! interval, one connector thread per peer carrying wire frames
//! over TCP with bounded outbound queues, and an accept loop that
//! routes each inbound connection by its first four bytes:
//! `"SRKV"` is a peer frame stream, anything else is a
//! length-prefixed client message.
//!
//! Persistence is synchronous in the driver: a persist action
//! is written and fsynced by the store before the driver steps
//! the acknowledgment that releases the dependent responses.
//! That is the persist-before-response contract of
//! docs/ARCHITECTURE.md section 4, kept literal.
//!
//! Client writes complete when the core's commit answer and
//! the state machine's apply outcome have both arrived, in
//! either order. Reads are served through `ReadIndex` and held
//! until the applied index reaches the read index. A leadership
//! loss fails parked client work with a retryable error; the
//! client implementation retries within its budget.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender, SyncSender, TrySendError, channel, sync_channel};
use std::time::Duration;

use simraft_core::{
    Action, ClientId, ClientResult, Config, EntryKind, Event, LogIndex, Message, Node, NodeId,
    PersistKind, SnapshotMeta,
};
use simraft_kv::machine::ERR_DECODE;
use simraft_kv::{
    ApplyOutcome, CommandResult, KvOp, KvStateMachine, decode_snapshot, encode_command,
    encode_snapshot,
};
use simraft_wire::{decode_frame, encode_frame, inspect_frame_header};

use crate::client::{ClientResponse, MAX_CLIENT_BODY_BYTES, decode_request, encode_response};
use crate::store::FileStore;
use crate::{ClientOp, NodeConfig, NodeError};

/// Entries past the log base that trigger a compaction check.
const COMPACT_THRESHOLD: u64 = 128;
/// Bounded capacity of the driver's inbound event queue.
const INBOX_CAPACITY: usize = 4_096;
/// Bounded capacity of each peer's outbound frame queue so a
/// disconnected peer cannot grow memory unboundedly.
const PEER_QUEUE_CAPACITY: usize = 256;
/// Read and write timeout applied to client sockets and partial
/// peer frames.
const SOCKET_IO_TIMEOUT: Duration = Duration::from_secs(10);
/// Write timeout applied to outbound peer sockets.
const PEER_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// One input to the driver loop.
enum Inbox {
    Tick,
    Peer {
        from: NodeId,
        msg: Message,
    },
    Client {
        client_id: ClientId,
        seq: u64,
        op: ClientOp,
        reply: Sender<ClientResponse>,
    },
}

/// The driver: the core, the machine, and the store, plus the
/// client work parked against them.
struct Driver {
    id: NodeId,
    node: Node,
    machine: KvStateMachine,
    store: FileStore,
    members: BTreeMap<NodeId, SocketAddr>,
    senders: BTreeMap<NodeId, SyncSender<Vec<u8>>>,
    pending_writes: BTreeMap<(ClientId, u64), Sender<ClientResponse>>,
    committed: BTreeSet<(ClientId, u64)>,
    outcomes: BTreeMap<(ClientId, u64), CommandResult>,
    pending_reads: BTreeMap<u64, Sender<ClientResponse>>,
    deferred_reads: Vec<(u64, Vec<u8>, LogIndex)>,
    next_request_id: u64,
    tick: u64,
    pending_compaction: Option<(SnapshotMeta, Vec<u8>)>,
}

impl Driver {
    fn leader_addr(&self, leader: Option<NodeId>) -> ClientResponse {
        ClientResponse::Redirect(leader.and_then(|id| self.members.get(&id).copied()))
    }

    /// Step the core with `event` and process every released
    /// action, following the acknowledgment chains (persist,
    /// snapshot) until the core is quiescent.
    fn step(&mut self, event: Event) -> Result<(), NodeError> {
        let mut queue: VecDeque<Event> = VecDeque::from([event]);
        while let Some(event) = queue.pop_front() {
            let actions = self.node.step(event);
            for action in actions {
                self.act(action, &mut queue)?;
            }
        }
        // An apply during the drain may have planned a
        // compaction; complete its persist cycle now that the
        // core is quiescent.
        if let Some((meta, data)) = self.pending_compaction.take() {
            self.run_compaction(meta, data)?;
        }
        Ok(())
    }

    /// Complete the persist cycle for this node's own
    /// snapshot (invariant A-05).
    fn run_compaction(&mut self, meta: SnapshotMeta, data: Vec<u8>) -> Result<(), NodeError> {
        let parts = decode_snapshot(&data).map_err(|e| NodeError::CorruptStore {
            detail: format!("this node's own snapshot payload does not decode: {e}"),
        })?;
        self.store.write_snapshot(&meta, &data)?;
        self.machine.install_snapshot(parts);
        self.step(Event::SnapshotLoaded {
            meta: meta.clone(),
            data,
        })?;
        self.step(Event::SnapshotPersisted { meta })
    }

    fn act(&mut self, action: Action, queue: &mut VecDeque<Event>) -> Result<(), NodeError> {
        match action {
            Action::Send { to, msg } => {
                if let Some(sender) = self.senders.get(&to)
                    && let Ok(frame) = encode_frame(self.id, to, &msg)
                {
                    // Non-blocking enqueue: if a down peer's bounded queue is
                    // full, dropping the frame keeps the driver responsive and
                    // memory bounded; Raft heartbeats and retries retransmit.
                    let _ = sender.try_send(frame);
                }
            }
            Action::PersistHardState {
                current_term,
                voted_for,
            } => {
                self.store.write_hard_state(current_term, voted_for)?;
                queue.push_back(Event::Persisted {
                    kind: PersistKind::HardState,
                    up_to_index: LogIndex::ZERO,
                });
            }
            Action::PersistEntries {
                from_index,
                entries,
            } => {
                let last = if entries.is_empty() {
                    from_index
                } else {
                    LogIndex(from_index.0 + u64::try_from(entries.len() - 1).unwrap_or(u64::MAX))
                };
                self.store.persist_entries(from_index, &entries)?;
                queue.push_back(Event::Persisted {
                    kind: PersistKind::Entries,
                    up_to_index: last,
                });
            }
            Action::PersistSnapshot { meta, data } => {
                let parts = decode_snapshot(&data).map_err(|e| NodeError::CorruptStore {
                    detail: format!("a snapshot payload from the core does not decode: {e}"),
                })?;
                self.store.write_snapshot(&meta, &data)?;
                self.machine.install_snapshot(parts);
                queue.push_back(Event::SnapshotPersisted { meta });
            }
            Action::LoadSnapshot { meta } => {
                if let Some(data) = self.snapshot_bytes(&meta) {
                    queue.push_back(Event::SnapshotLoaded { meta, data });
                }
            }
            Action::Apply { index, entry } => {
                self.apply_entry(index, &entry);
            }
            Action::ReadReady {
                request_id,
                key,
                read_index,
            } => {
                if let Some(value) = self.machine.read(read_index, &key) {
                    if let Some(reply) = self.pending_reads.remove(&request_id) {
                        let _ = reply.send(ClientResponse::Ok(value.map(<[u8]>::to_vec)));
                    }
                } else {
                    self.deferred_reads.push((request_id, key, read_index));
                }
            }
            Action::ReadRejected { request_id, leader } => {
                if let Some(reply) = self.pending_reads.remove(&request_id) {
                    let response = self.leader_addr(leader);
                    let _ = reply.send(response);
                }
            }
            Action::ClientResponse {
                client_id,
                seq,
                result,
            } => match result {
                ClientResult::Committed => {
                    self.committed.insert((client_id, seq));
                    self.try_complete_writes();
                }
                ClientResult::NotLeader { leader } => {
                    if let Some(reply) = self.pending_writes.remove(&(client_id, seq)) {
                        let response = self.leader_addr(leader);
                        let _ = reply.send(response);
                    }
                }
                ClientResult::Rejected { error } => {
                    if let Some(reply) = self.pending_writes.remove(&(client_id, seq)) {
                        let _ = reply.send(ClientResponse::Error(error.to_string()));
                    }
                }
            },
            Action::ConfigResponse { .. } => {}
            Action::StepDown { .. } => {
                self.fail_parked();
            }
        }
        Ok(())
    }

    /// Apply one committed entry to the state machine, record
    /// the client outcome it carries (including `DuplicateStale`
    /// and `Failed` outcomes so `committed` and `pending_writes`
    /// never leak or hang), and run follow-on work.
    fn apply_entry(&mut self, index: LogIndex, entry: &simraft_core::LogEntry) {
        if let EntryKind::Command { client_id, seq, .. } = &entry.kind {
            let key = (*client_id, *seq);
            let result = match self.machine.apply(index, entry) {
                Ok(ApplyOutcome::Applied { result } | ApplyOutcome::Cached { result }) => result,
                Ok(ApplyOutcome::DuplicateStale | ApplyOutcome::NotCommand) => CommandResult::Ack,
                Ok(ApplyOutcome::Failed { error }) | Err(error) => CommandResult::Error {
                    code: ERR_DECODE,
                    message: error.to_string(),
                },
            };
            self.outcomes.insert(key, result);
        } else {
            let _ = self.machine.apply(index, entry);
        }
        self.try_complete_writes();
        self.serve_deferred_reads();
        if let Some(plan) = self.compaction_plan() {
            self.pending_compaction = Some(plan);
        }
    }

    /// Fail every parked client operation with a retryable
    /// error (leadership was lost).
    fn fail_parked(&mut self) {
        let writes = std::mem::take(&mut self.pending_writes).into_values();
        let reads = std::mem::take(&mut self.pending_reads).into_values();
        self.deferred_reads.clear();
        self.committed.clear();
        self.outcomes.clear();
        for reply in writes.chain(reads) {
            let _ = reply.send(ClientResponse::Error("leadership lost; retry".to_string()));
        }
    }

    /// The snapshot payload for `meta`: the durable copy when
    /// it matches, otherwise rebuilt from the state machine
    /// fenced strictly to the boundary the machine stands on
    /// (invariant A-06).
    fn snapshot_bytes(&self, meta: &SnapshotMeta) -> Option<Vec<u8>> {
        if let Ok(Some((stored, data))) = self.store.read_snapshot()
            && stored == *meta
        {
            return Some(data);
        }
        if self.node.last_applied() == meta.last_included_index
            && self.node.log().term_at(meta.last_included_index).is_some()
        {
            let parts = self.machine.snapshot_parts(
                meta.last_included_index,
                meta.last_included_term,
                self.node.cluster(),
            );
            return encode_snapshot(&parts).ok();
        }
        None
    }

    fn try_complete_writes(&mut self) {
        let ready: Vec<(ClientId, u64)> = self
            .committed
            .iter()
            .filter(|key| self.outcomes.contains_key(*key))
            .copied()
            .collect();
        for key in ready {
            self.committed.remove(&key);
            if let (Some(reply), Some(result)) =
                (self.pending_writes.remove(&key), self.outcomes.remove(&key))
            {
                let response = match result {
                    CommandResult::Ack => ClientResponse::Ok(None),
                    CommandResult::Value(bytes) => ClientResponse::Ok(Some(bytes)),
                    CommandResult::Error { message, .. } => ClientResponse::Error(message),
                };
                let _ = reply.send(response);
            }
        }
    }

    fn serve_deferred_reads(&mut self) {
        let applied = self.node.last_applied();
        let mut still = Vec::new();
        for (request_id, key, read_index) in self.deferred_reads.drain(..) {
            if read_index <= applied {
                if let Some(reply) = self.pending_reads.remove(&request_id)
                    && let Some(value) = self.machine.read(read_index, &key)
                {
                    let _ = reply.send(ClientResponse::Ok(value.map(<[u8]>::to_vec)));
                }
            } else {
                still.push((request_id, key, read_index));
            }
        }
        self.deferred_reads = still;
    }

    /// The compaction plan for the current state, when the
    /// log past the base has reached the threshold and the
    /// machine has applied through the commit index.
    fn compaction_plan(&self) -> Option<(SnapshotMeta, Vec<u8>)> {
        let commit = self.node.commit_index();
        let base = self.node.log().base_index();
        if commit <= base
            || commit.0 - base.0 < COMPACT_THRESHOLD
            || self.node.last_applied() < commit
        {
            return None;
        }
        let term = self
            .node
            .log()
            .term_at(commit)
            .unwrap_or_else(|| self.node.log().base_term());
        let parts = self
            .machine
            .snapshot_parts(commit, term, self.node.cluster());
        let data = encode_snapshot(&parts).ok()?;
        let meta = SnapshotMeta {
            last_included_index: commit,
            last_included_term: term,
            config: self.node.cluster().clone(),
        };
        Some((meta, data))
    }

    fn handle(&mut self, inbox: Inbox) -> Result<(), NodeError> {
        match inbox {
            Inbox::Tick => {
                self.tick += 1;
                self.step(Event::Tick { now: self.tick })
            }
            Inbox::Peer { from, msg } => self.step(Event::Message { from, msg }),
            Inbox::Client {
                client_id,
                seq,
                op,
                reply,
            } => match op {
                ClientOp::Get { key } => {
                    let request_id = self.next_request_id;
                    self.next_request_id += 1;
                    self.pending_reads.insert(request_id, reply);
                    self.step(Event::ReadIndex { request_id, key })
                }
                ClientOp::Put { key, value } => {
                    self.propose(client_id, seq, &KvOp::Put { key, value }, reply)
                }
                ClientOp::Delete { key } => {
                    self.propose(client_id, seq, &KvOp::Delete { key }, reply)
                }
            },
        }
    }

    fn propose(
        &mut self,
        client_id: ClientId,
        seq: u64,
        op: &KvOp,
        reply: Sender<ClientResponse>,
    ) -> Result<(), NodeError> {
        let payload = match encode_command(client_id, seq, op) {
            Ok(payload) => payload,
            Err(error) => {
                let _ = reply.send(ClientResponse::Error(error.to_string()));
                return Ok(());
            }
        };
        if let Some(old) = self.pending_writes.insert((client_id, seq), reply) {
            let _ = old.send(ClientResponse::Error(
                "superseded by a retry of the same operation".to_string(),
            ));
        }
        self.step(Event::Propose {
            client_id,
            seq,
            payload,
        })
    }
}

/// Build the core node and the state machine and restore the
/// durable state into them.
fn boot(
    config: &NodeConfig,
    store: &mut FileStore,
    recovery: crate::store::Recovery,
) -> Result<(Node, KvStateMachine), NodeError> {
    let cluster = config.cluster_config()?;
    let core_config = Config {
        heartbeat_interval: 5,
        min_election_timeout: 50,
        max_election_timeout: 100,
        ..Config::default()
    };
    let max_sessions = core_config.max_sessions;
    let seed = config
        .id
        .0
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(1);
    let mut node =
        Node::new(config.id, core_config, cluster, seed).map_err(|e| NodeError::CoreConfig {
            reason: e.to_string(),
        })?;
    let mut machine = KvStateMachine::with_max_sessions(max_sessions);
    let mut boot_actions = Vec::new();
    let boot_snapshot = recovery.snapshot.clone();
    if let Some((_, data)) = &boot_snapshot {
        let parts = decode_snapshot(data).map_err(|e| NodeError::CorruptStore {
            detail: format!("the durable snapshot payload does not decode: {e}"),
        })?;
        machine.install_snapshot(parts);
    }
    boot_actions.extend(node.step(Event::Restored {
        hard_state: recovery.hard_state,
        entries: recovery.entries,
        snapshot: boot_snapshot.as_ref().map(|(meta, _)| meta.clone()),
    }));
    if let Some((meta, data)) = boot_snapshot {
        boot_actions.extend(node.step(Event::SnapshotLoaded { meta, data }));
    }
    for action in boot_actions {
        match action {
            Action::PersistHardState {
                current_term,
                voted_for,
            } => store.write_hard_state(current_term, voted_for)?,
            Action::PersistEntries {
                from_index,
                entries,
            } => store.persist_entries(from_index, &entries)?,
            Action::PersistSnapshot { meta, data } => store.write_snapshot(&meta, &data)?,
            _ => {}
        }
    }
    Ok((node, machine))
}

/// Run one node process until it is stopped, per the contract
/// in the crate documentation.
///
/// # Errors
/// Returns the first validation, configuration, or storage
/// failure, with context.
pub fn run_node(config: &NodeConfig) -> Result<(), NodeError> {
    config.validate()?;
    let (mut store, recovery) = FileStore::open(&config.data_dir)?;
    if let Some(torn) = &recovery.torn {
        eprintln!(
            "simraft-node: recovered a torn log tail at byte {} (index {}): stored CRC {:#010x}, computed {:#010x}; truncated",
            torn.byte_offset, torn.index, torn.expected_crc, torn.actual_crc
        );
    }
    let (node, machine) = boot(config, &mut store, recovery)?;
    let members: BTreeMap<NodeId, SocketAddr> =
        config.members.iter().map(|m| (m.id, m.addr)).collect();
    let (inbox_tx, inbox_rx) = sync_channel::<Inbox>(INBOX_CAPACITY);
    let mut senders: BTreeMap<NodeId, SyncSender<Vec<u8>>> = BTreeMap::new();
    for member in &config.members {
        if member.id == config.id {
            continue;
        }
        let (tx, rx) = sync_channel::<Vec<u8>>(PEER_QUEUE_CAPACITY);
        senders.insert(member.id, tx);
        let addr = member.addr;
        std::thread::spawn(move || peer_connector(addr, rx));
    }
    let mut driver = Driver {
        id: config.id,
        node,
        machine,
        store,
        members: members.clone(),
        senders,
        pending_writes: BTreeMap::new(),
        committed: BTreeSet::new(),
        outcomes: BTreeMap::new(),
        pending_reads: BTreeMap::new(),
        deferred_reads: Vec::new(),
        next_request_id: 1,
        tick: 0,
        pending_compaction: None,
    };
    {
        let tx = inbox_tx.clone();
        let tick_ms = config.tick_ms;
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_millis(tick_ms));
                match tx.try_send(Inbox::Tick) {
                    Ok(()) | Err(TrySendError::Full(_)) => {}
                    Err(TrySendError::Disconnected(_)) => return,
                }
            }
        });
    }
    let self_addr = members
        .get(&config.id)
        .copied()
        .ok_or(NodeError::SelfNotInCluster { id: config.id })?;
    let listener = TcpListener::bind(self_addr).map_err(|e| NodeError::Io {
        path: PathBuf::from(format!("listen {self_addr}")),
        source: e,
    })?;
    {
        let tx = inbox_tx.clone();
        std::thread::spawn(move || accept_loop(listener, tx));
    }
    drop(inbox_tx);
    while let Ok(inbox) = inbox_rx.recv() {
        driver.handle(inbox)?;
    }
    Ok(())
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "thread entry point: the value moves into the spawned thread and is owned there"
)]
fn peer_connector(addr: SocketAddr, rx: Receiver<Vec<u8>>) {
    let mut pending: Option<Vec<u8>> = None;
    loop {
        let first_frame = match pending.take() {
            Some(frame) => frame,
            None => match rx.recv() {
                Ok(frame) => frame,
                Err(_) => return,
            },
        };
        if let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_secs(2)) {
            let _ = stream.set_nodelay(true);
            let _ = stream.set_write_timeout(Some(PEER_WRITE_TIMEOUT));
            let _ = stream.set_read_timeout(Some(SOCKET_IO_TIMEOUT));
            if stream.write_all(&first_frame).is_err() {
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }
            loop {
                match rx.recv() {
                    Ok(frame) => {
                        if stream.write_all(&frame).is_err() {
                            pending = Some(frame);
                            break;
                        }
                    }
                    Err(_) => return,
                }
            }
        } else {
            // Drain stale frames accumulated while the peer is unreachable
            // so reconnecting sends fresh heartbeats rather than an old backlog.
            while rx.try_recv().is_ok() {}
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "thread entry point: the value moves into the spawned thread and is owned there"
)]
fn accept_loop(listener: TcpListener, inbox: SyncSender<Inbox>) {
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let inbox = inbox.clone();
                std::thread::spawn(move || handle_connection(stream, inbox));
            }
            Err(_) => std::thread::sleep(Duration::from_millis(10)),
        }
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "thread entry point: the sender moves into the connection handler"
)]
fn handle_connection(mut stream: TcpStream, inbox: SyncSender<Inbox>) {
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(SOCKET_IO_TIMEOUT));
    let _ = stream.set_write_timeout(Some(SOCKET_IO_TIMEOUT));
    let mut first = [0_u8; 4];
    if stream.read_exact(&mut first).is_err() {
        return;
    }
    if &first == b"SRKV" {
        peer_reader(stream, first.to_vec(), inbox);
        return;
    }
    // Client message: the first four bytes are its length.
    let len = u32::from_le_bytes(first) as usize;
    if len > MAX_CLIENT_BODY_BYTES {
        return;
    }
    let mut body = vec![0_u8; len];
    if stream.read_exact(&mut body).is_err() {
        return;
    }
    let Ok((client_id, seq, op)) = decode_request(&body) else {
        return;
    };
    let (reply_tx, reply_rx) = channel::<ClientResponse>();
    match inbox.try_send(Inbox::Client {
        client_id,
        seq,
        op,
        reply: reply_tx,
    }) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => {
            let _ = stream.write_all(&encode_response(&ClientResponse::Error(
                "node inbox busy; retry".to_string(),
            )));
            return;
        }
        Err(TrySendError::Disconnected(_)) => return,
    }
    if let Ok(response) = reply_rx.recv_timeout(Duration::from_secs(30)) {
        let _ = stream.write_all(&encode_response(&response));
    }
}

/// A reassembly buffer for a peer frame stream.
///
/// Bytes arrive in arbitrary chunks and whole frames come out
/// by their headers, however the stream split them. Bad magic
/// or a body length above [`simraft_wire::MAX_FRAME_BODY_LEN`]
/// (16 MiB) is rejected as soon as the header bytes arrive
/// without buffering the body.
#[derive(Debug, Default)]
pub struct FrameBuffer {
    buffer: Vec<u8>,
}

impl FrameBuffer {
    /// An empty buffer.
    #[must_use]
    pub const fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    /// Whether the buffer currently holds zero partial-frame bytes.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Append freshly read bytes.
    pub fn push(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    /// The next whole frame in the buffer, decoded, when one
    /// has fully arrived (or an immediate error when the header
    /// carries bad magic or an oversized length).
    ///
    /// # Errors
    /// Returns `simraft_wire::WireError` when the frame at the
    /// front of the buffer is oversized or corrupt.
    pub fn next_frame(
        &mut self,
    ) -> Option<Result<(NodeId, NodeId, Message), simraft_wire::WireError>> {
        let total = match inspect_frame_header(&self.buffer)? {
            Ok(total) => total,
            Err(err) => {
                self.buffer.clear();
                return Some(Err(err));
            }
        };
        if self.buffer.len() < total {
            return None;
        }
        let frame: Vec<u8> = self.buffer.drain(..total).collect();
        Some(decode_frame(&frame))
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "thread entry point: the value moves into the spawned thread and is owned there"
)]
fn peer_reader(mut stream: TcpStream, seed: Vec<u8>, inbox: SyncSender<Inbox>) {
    let mut frames = FrameBuffer::new();
    frames.push(&seed);
    let mut chunk = [0_u8; 16 * 1024];
    loop {
        while let Some(decoded) = frames.next_frame() {
            match decoded {
                Ok((from, _to, msg)) => {
                    if inbox.send(Inbox::Peer { from, msg }).is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
        match stream.read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => frames.push(&chunk[..n]),
            Err(err)
                if matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut)
                    && frames.is_empty() =>
            {
                // An idle peer link at a clean frame boundary stays open;
                // a stalled partial frame (!frames.is_empty()) falls through
                // and closes the socket.
            }
            Err(_) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use simraft_core::{ClusterConfig, Role, Term};

    fn pump(driver: &mut Driver, inbox: &Receiver<Vec<u8>>) {
        while let Ok(frame) = inbox.try_recv() {
            let (from, _to, msg) = decode_frame(&frame).expect("self frame decodes");
            driver
                .step(Event::Message { from, msg })
                .expect("self message steps");
        }
    }

    fn peer(driver: &mut Driver, from: NodeId, msg: Message) {
        driver
            .step(Event::Message { from, msg })
            .expect("peer message steps");
    }

    fn seat_leader(driver: &mut Driver, inbox: &Receiver<Vec<u8>>) {
        driver
            .step(Event::Tick { now: 1_000_000 })
            .expect("election tick steps");
        pump(driver, inbox);
        let term = driver.node.current_term();
        peer(
            driver,
            NodeId(2),
            Message::RequestPreVoteResponse {
                term,
                vote_granted: true,
            },
        );
        pump(driver, inbox);
        let term = driver.node.current_term();
        peer(
            driver,
            NodeId(2),
            Message::RequestVoteResponse {
                term,
                vote_granted: true,
            },
        );
        pump(driver, inbox);
        assert_eq!(
            driver.node.role(),
            Role::Leader,
            "the scripted election must seat the leader"
        );
    }

    fn applied_driver() -> (Driver, PathBuf) {
        static NEXT_DIR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = NEXT_DIR.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "simraft-node-runtime-test-{}-{id}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let (store, recovery) = FileStore::open(&dir).expect("store opens");
        let cluster = ClusterConfig::new([NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("cluster validates");
        let config = Config {
            heartbeat_interval: 5,
            min_election_timeout: 50,
            max_election_timeout: 100,
            ..Config::default()
        };
        let max_sessions = config.max_sessions;
        let node =
            Node::new(NodeId(1), config, cluster, 0x9E37_79B9_7F4A_7C15 + 1).expect("node builds");
        let (sender, inbox) = sync_channel::<Vec<u8>>(PEER_QUEUE_CAPACITY);
        let mut driver = Driver {
            id: NodeId(1),
            node,
            machine: KvStateMachine::with_max_sessions(max_sessions),
            store,
            members: BTreeMap::new(),
            senders: BTreeMap::from([(NodeId(1), sender)]),
            pending_writes: BTreeMap::new(),
            committed: BTreeSet::new(),
            outcomes: BTreeMap::new(),
            pending_reads: BTreeMap::new(),
            deferred_reads: Vec::new(),
            next_request_id: 1,
            tick: 0,
            pending_compaction: None,
        };
        driver
            .step(Event::Restored {
                hard_state: recovery.hard_state,
                entries: recovery.entries,
                snapshot: recovery.snapshot.map(|(meta, _)| meta),
            })
            .expect("restore steps");
        seat_leader(&mut driver, &inbox);
        for seq in 1..=4_u64 {
            let payload = encode_command(
                ClientId(7),
                seq,
                &KvOp::Put {
                    key: format!("key-{seq}").into_bytes(),
                    value: format!("value-{seq}").into_bytes(),
                },
            )
            .expect("command encodes");
            driver
                .step(Event::Propose {
                    client_id: ClientId(7),
                    seq,
                    payload,
                })
                .expect("proposal steps");
        }
        let term = driver.node.current_term();
        let last = driver.node.log().last_index();
        peer(
            &mut driver,
            NodeId(2),
            Message::AppendEntriesResponse {
                term,
                success: true,
                match_index: last,
                conflict_index: LogIndex::ZERO,
                conflict_term: Term::ZERO,
                read_round: 0,
            },
        );
        (driver, dir)
    }

    #[test]
    fn snapshot_rebuild_never_serves_later_state_under_an_earlier_boundary() {
        let (driver, dir) = applied_driver();
        let applied = driver.node.last_applied();
        assert!(
            applied.0 >= 3,
            "the driver must stand past an earlier boundary, applied {applied}"
        );
        assert!(
            driver.store.read_snapshot().expect("store reads").is_none(),
            "the test exercises the rebuild path, so the store holds no snapshot"
        );
        let cluster = driver.node.cluster().clone();
        let earlier = LogIndex(applied.0 - 2);
        let earlier_meta = SnapshotMeta {
            last_included_index: earlier,
            last_included_term: driver
                .node
                .log()
                .term_at(earlier)
                .expect("the earlier index has a term"),
            config: cluster.clone(),
        };
        assert!(
            driver.snapshot_bytes(&earlier_meta).is_none(),
            "no payload may be served for a boundary the machine has applied past"
        );
        let exact_meta = SnapshotMeta {
            last_included_index: applied,
            last_included_term: driver
                .node
                .log()
                .term_at(applied)
                .expect("the applied index has a term"),
            config: cluster,
        };
        let bytes = driver
            .snapshot_bytes(&exact_meta)
            .expect("the boundary the machine stands on is served");
        let parts = decode_snapshot(&bytes).expect("the served payload decodes");
        assert_eq!(parts.last_included_index, applied);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn duplicate_stale_and_failed_applies_complete_parked_writes_without_hanging() {
        let (mut driver, dir) = applied_driver();
        assert_eq!(
            driver.machine.max_sessions(),
            Config::default().max_sessions
        );

        // ClientId(7) already applied seq 1..=4 in applied_driver; proposing seq 1
        // again hits ApplyOutcome::DuplicateStale when committed.
        let (reply_tx, reply_rx) = channel();
        driver
            .propose(
                ClientId(7),
                1,
                &KvOp::Put {
                    key: b"stale".to_vec(),
                    value: b"ignored".to_vec(),
                },
                reply_tx,
            )
            .expect("stale proposal steps");
        let term = driver.node.current_term();
        let last = driver.node.log().last_index();
        peer(
            &mut driver,
            NodeId(2),
            Message::AppendEntriesResponse {
                term,
                success: true,
                match_index: last,
                conflict_index: LogIndex::ZERO,
                conflict_term: Term::ZERO,
                read_round: 0,
            },
        );
        let response = reply_rx
            .try_recv()
            .expect("DuplicateStale must immediately resolve parked write");
        assert_eq!(response, ClientResponse::Ok(None));
        assert!(driver.committed.is_empty());

        // Malformed command payload hits ApplyOutcome::Failed and resolves with Error.
        let (bad_tx, bad_rx) = channel();
        driver.pending_writes.insert((ClientId(99), 1), bad_tx);
        driver
            .step(Event::Propose {
                client_id: ClientId(99),
                seq: 1,
                payload: vec![0xFF],
            })
            .expect("bad payload proposal steps");
        let last = driver.node.log().last_index();
        peer(
            &mut driver,
            NodeId(2),
            Message::AppendEntriesResponse {
                term,
                success: true,
                match_index: last,
                conflict_index: LogIndex::ZERO,
                conflict_term: Term::ZERO,
                read_round: 0,
            },
        );
        let bad_resp = bad_rx
            .try_recv()
            .expect("Failed apply must immediately resolve parked write");
        assert!(matches!(bad_resp, ClientResponse::Error(_)));
        assert!(driver.committed.is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
