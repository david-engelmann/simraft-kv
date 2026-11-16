//! End-to-end behaviors of a real three-node cluster on
//! localhost (docs/ROADMAP.md Lane 1). These tests are the
//! lane's Stage A contract: they spawn the compiled
//! simraft-node binary, drive it through its client mode, and
//! assert the durability, failover, and recovery behaviors the
//! adapter promises in docs/INVARIANTS.md section 10 (rows A-01
//! to A-03). They fail at Stage A, where the binary validates
//! its configuration and reports that the runtime is not yet
//! implemented, and pass at Stage B.

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_simraft-node");

/// A running cluster of three node processes on ephemeral ports.
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
    let dir = std::env::temp_dir().join(format!("simraft-node-e2e-{tag}-{}", std::process::id()));
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

    /// Wait until every node process is serving. A node that
    /// exits first fails the test at once with its own output.
    fn wait_ready(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        for i in 0..self.children.len() {
            // Readiness is probed through the client contract:
            // poll a Get against each member until it answers or
            // the process exits.
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
                    panic!(
                        "node {} exited with {status} before serving; Stage A reports the runtime as not yet implemented",
                        i + 1
                    );
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

/// A-01: a write the client saw acknowledged survives killing
/// and restarting the leader, and the cluster keeps serving.
#[test]
fn writes_survive_leader_restart() {
    let mut cluster = Cluster::start("survive");
    client_put(&cluster.addrs[0], "k1", "v1").expect("the first write is acknowledged");
    cluster.kill(0);
    cluster.restart(0);
    cluster.wait_ready();
    let value = client_get(&cluster.addrs[1], "k1").expect("the value is still served");
    assert_eq!(value, "v1", "A-01: an acknowledged write survives");
    client_put(&cluster.addrs[1], "k2", "v2").expect("the cluster keeps serving writes");
}

/// A-02: after a failover, a read returns the latest committed
/// value, never an older one.
#[test]
fn read_after_failover_returns_latest_committed() {
    let mut cluster = Cluster::start("failover");
    client_put(&cluster.addrs[0], "k", "first").expect("the first write commits");
    client_put(&cluster.addrs[0], "k", "second").expect("the second write commits");
    cluster.kill(0);
    // The surviving members elect a successor; reads through any
    // of them must observe the second value.
    let value = client_get(&cluster.addrs[1], "k")
        .expect("a read through a survivor is served after the failover");
    assert_eq!(
        value, "second",
        "A-02: a post-failover read returns the latest committed value"
    );
}

/// A-03: a node restarted after falling behind rejoins, catches
/// up, and serves the current state through its own address.
#[test]
fn restarted_node_catches_up() {
    let mut cluster = Cluster::start("catchup");
    cluster.kill(2);
    client_put(&cluster.addrs[0], "k", "while-third-was-down")
        .expect("writes continue on the majority");
    cluster.restart(2);
    cluster.wait_ready();
    let value = client_get(&cluster.addrs[2], "k")
        .expect("the restarted node serves through its own address");
    assert_eq!(
        value, "while-third-was-down",
        "A-03: the restarted node caught up to the current state"
    );
}
