//! A-05: a node that catches up through a leader snapshot
//! serves the snapshotted state from its own state machine
//! (docs/INVARIANTS.md section 10). The scenario crosses the
//! adapter's compaction threshold while one member is down,
//! so the member's only route back is a snapshot, then wipes
//! the other two members so the snapshot receiver is the only
//! node holding the committed state and must lead. Reads are
//! then served from the receiver's own machine: the client
//! follows redirects, so only a leader's machine is ever read,
//! and promoting the receiver is what makes its machine the
//! serving one. Before the fix this test is red at the first
//! promoted read, because the receiver persisted the snapshot
//! to its store but never installed it into its running state
//! machine, and only a restart (the boot path) healed it.

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_simraft-node");

/// Writes performed while the third member is down. The
/// adapter compacts a log once the commit index passes the
/// base by 128 entries, so 140 writes put the earliest keys
/// beyond any entry-by-entry catch-up.
const WRITES: u32 = 140;

/// A running cluster of three node processes on ephemeral
/// ports, modeled on cluster_end_to_end.rs.
struct Cluster {
    children: Vec<Child>,
    addrs: Vec<String>,
    dirs: Vec<PathBuf>,
}

fn free_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    let addr = listener.local_addr().expect("the listener has an address");
    drop(listener);
    addr.to_string()
}

fn data_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("simraft-snap-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

impl Cluster {
    fn start(tag: &str) -> Self {
        let addrs: Vec<String> = (0..3).map(|_| free_addr()).collect();
        let specs: Vec<String> = addrs
            .iter()
            .enumerate()
            .map(|(i, addr)| format!("{}@{}", i + 1, addr))
            .collect();
        let mut children = Vec::new();
        let mut dirs = Vec::new();
        for i in 0..3 {
            let dir = data_dir(&format!("{tag}-{i}"));
            let mut cmd = Command::new(BIN);
            cmd.arg("serve")
                .arg("--id")
                .arg(format!("{}", i + 1))
                .arg("--data-dir")
                .arg(&dir)
                .arg("--tick-ms")
                .arg("10")
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            for spec in &specs {
                cmd.arg("--member").arg(spec);
            }
            children.push(cmd.spawn().expect("spawn a node process"));
            dirs.push(dir);
        }
        let mut cluster = Self {
            children,
            addrs,
            dirs,
        };
        cluster.wait_ready();
        cluster
    }

    /// Wait until every node process is serving, probing
    /// through the client contract as the end-to-end suite
    /// does.
    fn wait_ready(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        for i in 0..self.children.len() {
            loop {
                assert!(
                    Instant::now() < deadline,
                    "node {} did not become ready in time",
                    i + 1
                );
                if client_get(&self.addrs[i], "readiness-probe").is_ok() {
                    break;
                }
                if let Some(status) = self.children[i].try_wait().expect("poll the node process") {
                    panic!("node {} exited with {status} before serving", i + 1);
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }

    fn kill(&mut self, index: usize) {
        self.children[index].kill().expect("kill the node");
        self.children[index].wait().expect("reap the node");
    }

    fn restart(&mut self, index: usize) {
        let specs: Vec<String> = self
            .addrs
            .iter()
            .enumerate()
            .map(|(i, addr)| format!("{}@{}", i + 1, addr))
            .collect();
        let mut cmd = Command::new(BIN);
        cmd.arg("serve")
            .arg("--id")
            .arg(format!("{}", index + 1))
            .arg("--data-dir")
            .arg(&self.dirs[index])
            .arg("--tick-ms")
            .arg("10")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for spec in &specs {
            cmd.arg("--member").arg(spec);
        }
        self.children[index] = cmd.spawn().expect("respawn the node");
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
        for dir in &self.dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

fn client(args: &[&str]) -> Result<String, String> {
    let output = Command::new(BIN)
        .arg("client")
        .args(args)
        .output()
        .expect("run the client mode");
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_string())
    }
}

fn client_put(server: &str, key: &str, value: &str) -> Result<(), String> {
    client(&["--server", server, "put", key, value]).map(|_| ())
}

fn client_get(server: &str, key: &str) -> Result<String, String> {
    client(&["--server", server, "get", key])
}

/// Poll until a node's store holds a snapshot file, the
/// durable sign that a compaction or a received install
/// completed on that node.
fn wait_for_snapshot(cluster: &Cluster, index: usize, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    let path = cluster.dirs[index].join("snapshot");
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "node {} never persisted a snapshot ({what})",
            index + 1
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A-05: the snapshot receiver serves snapshotted keys from
/// its own machine once it leads, with no restart in between.
#[test]
fn snapshot_receiver_serves_snapshotted_state_after_promotion() {
    let mut cluster = Cluster::start("install");
    cluster.kill(2);
    for i in 1..=WRITES {
        let key = format!("key-{i:04}");
        let value = format!("v{i:04}");
        client_put(&cluster.addrs[0], &key, &value).expect("writes commit on the majority");
    }
    // The leader must have compacted before the third member
    // returns, so the member's catch-up is a snapshot, not a
    // run of entries.
    wait_for_snapshot(&cluster, 0, "leader compaction");
    cluster.restart(2);
    wait_for_snapshot(&cluster, 2, "received snapshot");
    // One more write lands on the receiver as ordinary
    // entries past the snapshot boundary.
    client_put(&cluster.addrs[0], "probe", "after-snapshot").expect("the cluster is healthy");
    std::thread::sleep(Duration::from_millis(500));

    // Promote the receiver: wipe the other two members so
    // the receiver's log is the only complete one and it
    // must lead the re-formed cluster.
    cluster.kill(0);
    cluster.kill(1);
    for index in 0..2 {
        std::fs::remove_dir_all(&cluster.dirs[index]).expect("wipe a member data dir");
    }
    cluster.restart(0);
    cluster.restart(1);
    cluster.wait_ready();

    let first = client_get(&cluster.addrs[2], "key-0001").expect("the promoted node serves reads");
    assert_eq!(
        first, "v0001",
        "A-05: the snapshot receiver serves a snapshotted key from its own machine"
    );
    let middle = client_get(&cluster.addrs[2], "key-0100").expect("the promoted node serves reads");
    assert_eq!(
        middle, "v0100",
        "A-05: the snapshot receiver serves a snapshotted key from its own machine"
    );
    let probe = client_get(&cluster.addrs[2], "probe").expect("the promoted node serves reads");
    assert_eq!(
        probe, "after-snapshot",
        "A-05: entries applied past the snapshot boundary still serve"
    );
    client_put(&cluster.addrs[2], "fresh", "after-promotion").expect("the promoted node leads");
    let fresh = client_get(&cluster.addrs[0], "fresh").expect("the re-formed cluster serves");
    assert_eq!(
        fresh, "after-promotion",
        "A-05: the re-formed cluster keeps serving new writes"
    );
}
