//! Adapter chaos, end to end (invariants A-07, A-08, A-09):
//! the real node binary under process-level faults. Three
//! scenarios: a torn log tail (a node whose durable log ends in
//! a partially written record boots, truncates the tail at the
//! first bad record, and rejoins with the entries before the
//! tear intact, A-07); a leader killed in the middle of a write
//! burst (no acknowledged write is lost, A-08); and a member
//! whose whole data directory is wiped rejoining fresh and
//! converging through a snapshot while writes continue (A-09).

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_simraft-node");

static CLUSTER_LOCK: Mutex<()> = Mutex::new(());

struct Cluster {
    children: Vec<Child>,
    addrs: Vec<String>,
    dirs: Vec<PathBuf>,
    _lock: MutexGuard<'static, ()>,
}

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
    let dir = std::env::temp_dir().join(format!("simraft-node-chaos-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
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
        self.wait_ready_some(&[0, 1, 2]);
    }

    fn wait_ready_some(&mut self, indices: &[usize]) {
        if let Err(err) = wait_children_ready(&mut self.children, &self.addrs, indices) {
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
    // Wait until node 3 has persisted at least one log record before killing it.
    let log_path = cluster.dirs[2].join("log");
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::fs::metadata(&log_path).map_or(0, |m| m.len()) <= 12 {
        assert!(Instant::now() < deadline, "node 3 never wrote its log file");
        std::thread::sleep(Duration::from_millis(25));
    }
    cluster.kill(2);
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

/// A-08: during a continuous write burst against the
/// leader, killing the leader loses no write the client
/// saw acknowledged; after the failover every acknowledged
/// key reads back with its exact value through a survivor.
#[test]
#[allow(
    clippy::significant_drop_tightening,
    reason = "the cluster guard is the serialization lock and must live until the cluster is torn down at the end of the test"
)]
fn acknowledged_writes_survive_leader_kill_mid_burst() {
    let mut cluster = Cluster::start("burst");
    let addr = cluster.addrs[0].clone();
    let (tx, rx) = std::sync::mpsc::channel::<(String, String)>();
    let writer = std::thread::spawn(move || {
        for i in 1..=200_u32 {
            let key = format!("burst-{i:04}");
            let value = format!("v{i:04}");
            if client_put(&addr, &key, &value).is_err() {
                break;
            }
            let _ = tx.send((key, value));
        }
    });
    let mut acked: Vec<(String, String)> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while acked.len() < 3 {
        assert!(
            Instant::now() < deadline,
            "the burst landed no acknowledged writes to kill behind"
        );
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(pair) => acked.push(pair),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    cluster.kill(0);
    writer.join().expect("the writer thread finishes");
    acked.extend(rx.try_iter());
    cluster.wait_ready_some(&[1]);
    for (key, value) in &acked {
        let got = client_get(&cluster.addrs[1], key).expect("a survivor serves after failover");
        assert_eq!(
            &got, value,
            "A-08: an acknowledged write survives the leader kill"
        );
    }
    client_put(&cluster.addrs[1], "after", "failover").expect("the cluster keeps serving writes");
}

/// A-09: a member whose entire data directory is wiped
/// rejoins as a fresh node with the same id, converges
/// through a snapshot while writes continue, and, once
/// promoted, serves the full current state, including the
/// writes that landed during its transfer.
#[test]
#[allow(
    clippy::significant_drop_tightening,
    reason = "the cluster guard is the serialization lock and must live until the cluster is torn down at the end of the test"
)]
fn wiped_member_rejoins_fresh_and_converges_under_load() {
    let mut cluster = Cluster::start("wiped");
    cluster.kill(2);
    for i in 1..=140_u32 {
        let key = format!("key-{i:04}");
        let value = format!("v{i:04}");
        client_put(&cluster.addrs[0], &key, &value).expect("writes commit on the majority");
    }
    wait_for_snapshot(&cluster, 0, "leader compaction");
    wait_for_snapshot(&cluster, 1, "follower compaction");
    std::fs::remove_dir_all(&cluster.dirs[2]).expect("wipe the member data dir");
    cluster.restart(2);
    for i in 141..=160_u32 {
        let key = format!("key-{i:04}");
        let value = format!("v{i:04}");
        client_put(&cluster.addrs[0], &key, &value).expect("writes continue during the transfer");
    }
    wait_for_snapshot(&cluster, 2, "received snapshot");
    cluster.wait_ready_some(&[2]);

    cluster.kill(0);
    cluster.kill(1);
    std::fs::remove_dir_all(&cluster.dirs[1]).expect("wipe a member data dir");
    cluster.restart(1);
    cluster.wait_ready_some(&[1, 2]);

    let first = client_get(&cluster.addrs[2], "key-0001").expect("the promoted member serves");
    assert_eq!(
        first, "v0001",
        "A-09: a pre-wipe key serves from the rejoined member's own machine"
    );
    let during = client_get(&cluster.addrs[2], "key-0160").expect("the promoted member serves");
    assert_eq!(
        during, "v0160",
        "A-09: a write that landed during the transfer is present"
    );
    client_put(&cluster.addrs[2], "fresh", "after-promotion").expect("the promoted member leads");
}
