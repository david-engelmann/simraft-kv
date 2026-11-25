//! End-to-end behaviors of a real three-node cluster on
//! localhost (docs/INVARIANTS.md section 10, rows A-01 to A-03,
//! plus session collision resistance and CLI exit code contracts).

use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use simraft_node::{Client, ClientOp, run_client};

const BIN: &str = env!("CARGO_BIN_EXE_simraft-node");

static CLUSTER_LOCK: Mutex<()> = Mutex::new(());

/// A running cluster of three node processes on ephemeral ports.
struct Cluster {
    children: Vec<Child>,
    addrs: Vec<String>,
    dirs: Vec<PathBuf>,
    _lock: MutexGuard<'static, ()>,
}

/// Bind `count` ephemeral listeners simultaneously before dropping
/// any of them so the OS never hands out duplicate ports within one
/// cluster.
fn reserve_free_addrs(count: usize) -> Vec<String> {
    let listeners: Vec<TcpListener> = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port"))
        .collect();
    listeners
        .iter()
        .map(|l| l.local_addr().expect("listener has local_addr").to_string())
        .collect()
}

fn data_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("simraft-node-e2e-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn spawn_nodes(tag: &str, attempt: usize, addrs: &[String]) -> (Vec<Child>, Vec<PathBuf>) {
    let specs: Vec<String> = addrs
        .iter()
        .enumerate()
        .map(|(i, addr)| format!("{}@{}", i + 1, addr))
        .collect();
    let mut children = Vec::new();
    let mut dirs = Vec::new();
    for i in 0..3 {
        let dir = data_dir(&format!("{tag}-{attempt}-{i}"));
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
    (children, dirs)
}

fn cleanup_attempt(children: &mut [Child], dirs: &[PathBuf]) {
    for child in children {
        let _ = child.kill();
        let _ = child.wait();
    }
    for dir in dirs {
        let _ = std::fs::remove_dir_all(dir);
    }
}

fn wait_children_ready(
    children: &mut [Child],
    addrs: &[String],
    indices: &[usize],
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    for &i in indices {
        loop {
            if Instant::now() >= deadline {
                return Err(format!("node {} did not become ready in time", i + 1));
            }
            if client_probe(&addrs[i]) {
                break;
            }
            if let Some(status) = children[i].try_wait().expect("poll the node process") {
                return Err(format!(
                    "node {} exited with {status} before serving",
                    i + 1
                ));
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    Ok(())
}

impl Cluster {
    fn start(tag: &str) -> Self {
        let probe = TcpListener::bind("127.0.0.1:0").expect("bind loopback probe listener");
        let probe_addr = probe.local_addr().expect("loopback listener local_addr");
        if let Err(err) = std::net::TcpStream::connect_timeout(&probe_addr, Duration::from_secs(1))
        {
            panic!(
                "loopback TCP connect to {probe_addr} failed ({err}); multi-process simraft-node TCP tests require loopback socket access"
            );
        }
        drop(probe);
        let lock = CLUSTER_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut last_err = String::new();
        for attempt in 0..5 {
            let addrs = reserve_free_addrs(3);
            let (mut children, dirs) = spawn_nodes(tag, attempt, &addrs);
            match wait_children_ready(&mut children, &addrs, &[0, 1, 2]) {
                Ok(()) => {
                    return Self {
                        children,
                        addrs,
                        dirs,
                        _lock: lock,
                    };
                }
                Err(err) => {
                    cleanup_attempt(&mut children, &dirs);
                    last_err = err;
                }
            }
        }
        panic!("cluster failed to start after 5 attempts: {last_err}");
    }

    fn wait_ready(&mut self) {
        if let Err(err) = wait_children_ready(&mut self.children, &self.addrs, &[0, 1, 2]) {
            panic!("{err}");
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
        cleanup_attempt(&mut self.children, &self.dirs);
    }
}

/// Probe whether `server` can complete a linearizable `ReadIndex`
/// read: exit code 0 (key present) and exit code 1 (key confirmed
/// absent) both prove the node is serving.
fn client_probe(server: &str) -> bool {
    let output = Command::new(BIN)
        .args(["client", "--server", server, "get", "readiness-probe"])
        .output()
        .expect("run the client probe");
    matches!(output.status.code(), Some(0 | 1))
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
#[allow(
    clippy::significant_drop_tightening,
    reason = "the cluster guard is the serialization lock and must live until the cluster is torn down at the end of the test"
)]
fn read_after_failover_returns_latest_committed() {
    let mut cluster = Cluster::start("failover");
    client_put(&cluster.addrs[0], "k", "first").expect("the first write commits");
    client_put(&cluster.addrs[0], "k", "second").expect("the second write commits");
    cluster.kill(0);
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
#[allow(
    clippy::significant_drop_tightening,
    reason = "the cluster guard is the serialization lock and must live until the cluster is torn down at the end of the test"
)]
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

#[test]
#[allow(
    clippy::significant_drop_tightening,
    reason = "the cluster guard is the serialization lock and must live until the cluster is torn down at the end of the test"
)]
fn rapid_sequential_run_client_puts_all_persist_without_collision() {
    let cluster = Cluster::start("rapid-puts");
    let server: SocketAddr = cluster.addrs[0].parse().expect("valid socket addr");

    // 20 rapid sequential one-shot `run_client` puts in the same process:
    // none may share (client_id, seq = 1) and silently drop a write.
    for i in 1..=20_u32 {
        let key = format!("rapid-{i:02}").into_bytes();
        let val = format!("val-{i:02}").into_bytes();
        run_client(server, &ClientOp::Put { key, value: val })
            .unwrap_or_else(|e| panic!("rapid put {i} failed: {e}"));
    }
    for i in 1..=20_u32 {
        let key = format!("rapid-{i:02}").into_bytes();
        let expected = format!("val-{i:02}").into_bytes();
        let got = run_client(server, &ClientOp::Get { key })
            .unwrap_or_else(|e| panic!("rapid get {i} failed: {e}"));
        assert_eq!(
            got.as_deref(),
            Some(expected.as_slice()),
            "rapid put {i} must not be dropped by session collision"
        );
    }

    // Also verify stateful `Client` session across Put -> Get -> Put -> Delete -> Get.
    let mut session = Client::new(server);
    session
        .put(b"session-key".to_vec(), b"alpha".to_vec())
        .expect("session put 1");
    assert_eq!(
        session.get(b"session-key".to_vec()).expect("session get 1"),
        Some(b"alpha".to_vec())
    );
    session
        .put(b"session-key".to_vec(), b"beta".to_vec())
        .expect("session put 2");
    assert_eq!(
        session.get(b"session-key".to_vec()).expect("session get 2"),
        Some(b"beta".to_vec())
    );
    session
        .delete(b"session-key".to_vec())
        .expect("session delete");
    assert_eq!(
        session.get(b"session-key".to_vec()).expect("session get 3"),
        None
    );
}

#[test]
#[allow(
    clippy::significant_drop_tightening,
    reason = "the cluster guard is the serialization lock and must live until the cluster is torn down at the end of the test"
)]
fn cli_get_distinguishes_missing_key_from_ok_value() {
    let cluster = Cluster::start("cli-ux");
    let server = &cluster.addrs[0];

    let missing = Command::new(BIN)
        .args(["client", "--server", server, "get", "absent"])
        .output()
        .expect("run get on missing key");
    assert_eq!(
        missing.status.code(),
        Some(1),
        "missing key must exit with code 1"
    );
    assert!(
        missing.stdout.is_empty(),
        "missing key must print nothing to stdout"
    );
    assert!(
        String::from_utf8_lossy(&missing.stderr).contains("key not found"),
        "missing key stderr must report key not found"
    );

    client_put(server, "literal-ok", "ok").expect("put literal ok");
    let present = Command::new(BIN)
        .args(["client", "--server", server, "get", "literal-ok"])
        .output()
        .expect("run get on literal-ok");
    assert_eq!(present.status.code(), Some(0));
    assert_eq!(String::from_utf8_lossy(&present.stdout).trim(), "ok");

    let deleted = Command::new(BIN)
        .args(["client", "--server", server, "delete", "literal-ok"])
        .output()
        .expect("run delete");
    assert_eq!(deleted.status.code(), Some(0));
    assert_eq!(String::from_utf8_lossy(&deleted.stdout).trim(), "ok");

    let after_delete = Command::new(BIN)
        .args(["client", "--server", server, "get", "literal-ok"])
        .output()
        .expect("run get after delete");
    assert_eq!(after_delete.status.code(), Some(1));
}
