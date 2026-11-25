//! Adapter chaos, end to end (register item R-2): the real
//! node binary under process-level faults, beyond the three
//! contract tests of Lane 1. This file's scenario is the
//! torn log tail: a node whose durable log ends in a
//! partially written record must boot, truncate the tail at
//! the first bad record, and rejoin with the entries before
//! the tear intact (invariant A-07). The store-level
//! recovery contract promises exactly this; whether it
//! holds through a running cluster is what this test pins.

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_simraft-node");

/// One cluster at a time across this binary's tests, as in
/// the Lane 1 end-to-end suite.
static CLUSTER_LOCK: Mutex<()> = Mutex::new(());

/// A running cluster of three node processes on ephemeral ports.
struct Cluster {
    children: Vec<Child>,
    addrs: Vec<String>,
    dirs: Vec<PathBuf>,
    _lock: MutexGuard<'static, ()>,
}

fn free_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    let addr = listener.local_addr().expect("the listener has an address");
    drop(listener);
    addr.to_string()
}

fn data_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("simraft-node-chaos-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

impl Cluster {
    fn start(tag: &str) -> Self {
        let lock = CLUSTER_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
            _lock: lock,
        };
        cluster.wait_ready();
        cluster
    }

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

/// A-07: a node whose log file ends in a torn record boots
/// on the truncated log, rejoins, and serves the state the
/// intact prefix held.
#[test]
#[allow(
    clippy::significant_drop_tightening,
    reason = "the cluster guard is the serialization lock and must live until the cluster is torn down at the end of the test"
)]
fn torn_log_tail_boots_truncates_and_rejoins() {
    let mut cluster = Cluster::start("torn");
    client_put(&cluster.addrs[0], "k", "before-the-tear").expect("the write is acknowledged");
    cluster.kill(2);
    // Tear the down node's log: append the first bytes of a
    // record from the head of the file, cut off mid-record,
    // the on-disk shape of a write interrupted by a crash.
    let log_path = cluster.dirs[2].join("log");
    let log_bytes = std::fs::read(&log_path).expect("the log file reads");
    assert!(log_bytes.len() > 12, "the log holds at least one record");
    let mut torn = log_bytes.clone();
    torn.extend_from_slice(&log_bytes[..12]);
    std::fs::write(&log_path, &torn).expect("the torn log writes");
    cluster.restart(2);
    cluster.wait_ready();
    let value = client_get(&cluster.addrs[2], "k")
        .expect("the torn node serves through its own address after rejoining");
    assert_eq!(
        value, "before-the-tear",
        "A-07: the state before the tear survives it"
    );
    client_put(&cluster.addrs[2], "k2", "after-rejoin").expect("the rejoined node takes writes");
}
