//! A-05: a node that catches up through a leader snapshot
//! serves the snapshotted state from its own state machine
//! (docs/INVARIANTS.md section 10).

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

struct Cluster {
    children: Vec<Child>,
    addrs: Vec<String>,
    dirs: Vec<PathBuf>,
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
    let dir = std::env::temp_dir().join(format!("simraft-snap-{tag}-{}", std::process::id()));
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
    wait_for_snapshot(&cluster, 0, "leader compaction");
    wait_for_snapshot(&cluster, 1, "follower compaction");
    cluster.restart(2);
    wait_for_snapshot(&cluster, 2, "received snapshot");
    client_put(&cluster.addrs[0], "probe", "after-snapshot").expect("the cluster is healthy");
    cluster.wait_ready_some(&[2]);

    cluster.kill(0);
    cluster.kill(1);
    std::fs::remove_dir_all(&cluster.dirs[1]).expect("wipe a member data dir");
    cluster.restart(1);
    cluster.wait_ready_some(&[1, 2]);

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
    let fresh = client_get(&cluster.addrs[1], "fresh").expect("the re-formed cluster serves");
    assert_eq!(
        fresh, "after-promotion",
        "A-05: the re-formed cluster keeps serving new writes"
    );
}
