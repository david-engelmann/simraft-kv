from __future__ import annotations

import errno
import hashlib
import importlib.util
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tarfile
import tempfile
import threading
import unittest
import unittest.mock

ROOT = Path(__file__).resolve().parents[2]
if importlib.util.find_spec("simraft_kv") is None:
    sys.path.insert(0, str(ROOT / "python"))

import simraft_kv  # noqa: E402


def _loopback_tcp_blocked() -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            addr = listener.getsockname()
            with socket.create_connection(addr, timeout=0.5):
                pass
        return False
    except PermissionError:
        return True
    except OSError as exc:
        if exc.errno in (errno.EPERM, errno.EACCES):
            return True
        raise


def _has_node_binary() -> bool:
    try:
        simraft_kv.find_node_binary()
        return True
    except FileNotFoundError:
        return False


def _frame_response(
    status: int, present: int = 0, data: bytes = b""
) -> bytes:
    body = b"SRCL" + struct.pack("<HBBI", 1, status, present, len(data)) + data
    return struct.pack("<I", len(body)) + body


def _read_exact(sock: socket.socket, n: int) -> bytes:
    out = bytearray()
    while len(out) < n:
        chunk = sock.recv(n - len(out))
        if not chunk:
            raise EOFError("unexpected EOF")
        out.extend(chunk)
    return bytes(out)


def _decode_wire_request(
    stream: socket.socket,
) -> tuple[int, int, int, bytes, bytes | None]:
    (body_len,) = struct.unpack("<I", _read_exact(stream, 4))
    body = _read_exact(stream, body_len)
    magic, ver, op, cid, seq, key_len = struct.unpack("<4sHBQQI", body[:27])
    assert magic == b"SRCL" and ver == 1
    key = body[27 : 27 + key_len]
    pos = 27 + key_len
    if op == 1:
        (val_len,) = struct.unpack("<I", body[pos : pos + 4])
        pos += 4
        val = body[pos : pos + val_len]
        pos += val_len
        assert pos == len(body)
        return op, cid, seq, key, val
    assert pos == len(body)
    return op, cid, seq, key, None


def _reserve_loopback_ports(count: int) -> list[int]:
    sockets: list[socket.socket] = []
    ports: list[int] = []
    try:
        for _ in range(count):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", 0))
            ports.append(sock.getsockname()[1])
            sockets.append(sock)
    finally:
        for sock in sockets:
            sock.close()
    return ports


class SimRaftKvSdkTests(unittest.TestCase):
    def test_version_matches_package(self) -> None:
        self.assertEqual(simraft_kv.__version__, "0.2.2")
        self.assertEqual(simraft_kv.version(), simraft_kv.__version__)

    def test_completions_cover_all_shells_for_sim_and_node(self) -> None:
        self.assertIn("completions", simraft_kv.__all__)
        shells = ("bash", "zsh", "fish", "powershell")

        for shell in shells:
            with self.subTest(tool="simraft-sim", shell=shell):
                script = simraft_kv.completions(shell, tool="simraft-sim")
                self.assertEqual(
                    script, simraft_kv.completions(shell, tool="simraft-sim")
                )
                for flag in (
                    "--seed",
                    "--seeds",
                    "--json",
                    "--trace",
                    "--chaos",
                    "--nodes",
                    "--ticks",
                    "--completions",
                ):
                    self.assertIn(flag, script)

        self.assertEqual(
            simraft_kv.completions(),
            simraft_kv.completions("bash", tool="simraft-sim"),
        )

        if _has_node_binary():
            for shell in shells:
                with self.subTest(tool="simraft-node", shell=shell):
                    script = simraft_kv.completions(shell, tool="simraft-node")
                    self.assertEqual(
                        script,
                        simraft_kv.completions(shell, tool="simraft-node"),
                    )
                    for token in (
                        "serve",
                        "--id",
                        "--member",
                        "--data-dir",
                        "--tick-ms",
                        "client",
                        "--server",
                        "put",
                        "get",
                        "delete",
                        "completions",
                    ):
                        self.assertIn(token, script)

        with self.assertRaises(ValueError):
            simraft_kv.completions("tcsh")
        with self.assertRaises(ValueError):
            simraft_kv.completions("bash", tool="unknown-tool")
        with self.assertRaises(TypeError):
            simraft_kv.completions(123)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            simraft_kv.completions("bash", tool=123)  # type: ignore[arg-type]

    @unittest.skipIf(os.name == "nt", "POSIX install.sh targets Linux and macOS")
    @unittest.skipUnless(
        _has_node_binary(),
        "simraft-node binary is not built or installed",
    )
    def test_install_sh_offline_sha256_verification_and_fail_closed(
        self,
    ) -> None:
        install_script = ROOT / "install.sh"
        self.assertTrue(install_script.is_file(), "install.sh must exist")
        self.assertTrue(
            os.access(install_script, os.X_OK),
            "install.sh must be executable",
        )

        help_proc = subprocess.run(
            ["/bin/sh", str(install_script), "--help"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(help_proc.returncode, 0, help_proc.stderr)
        self.assertIn("--version", help_proc.stdout)
        self.assertIn("--install-dir", help_proc.stdout)
        self.assertIn("--bin", help_proc.stdout)
        self.assertIn("--base-url", help_proc.stdout)

        sim_bin = simraft_kv.find_binary()
        node_bin = simraft_kv.find_node_binary()
        target = "x86_64-unknown-linux-gnu"
        tag = "v0.2.2"

        with tempfile.TemporaryDirectory(prefix="simraft-install-test-") as tmp:
            tmp_path = Path(tmp)
            rel_dir = tmp_path / "releases" / "download" / tag
            rel_dir.mkdir(parents=True, exist_ok=True)

            sums_lines: list[str] = []
            for name, src_bin in (
                ("simraft-node", node_bin),
                ("simraft-sim", sim_bin),
            ):
                archive_name = f"{name}-{tag}-{target}.tar.gz"
                archive_path = rel_dir / archive_name
                with tarfile.open(archive_path, "w:gz") as tar:
                    tar.add(src_bin, arcname=name)
                    for doc_name in (
                        "LICENSE-MIT",
                        "LICENSE-APACHE",
                        "README.md",
                    ):
                        tar.add(ROOT / doc_name, arcname=doc_name)
                digest = hashlib.sha256(archive_path.read_bytes()).hexdigest()
                sums_lines.append(f"{digest}  {archive_name}")

            sums_file = rel_dir / "SHA256SUMS"
            sums_file.write_text("\n".join(sums_lines) + "\n", encoding="utf-8")

            dest_ok = tmp_path / "bin-ok"
            env = {**os.environ, "SIMRAFT_TARGET": target}
            ok_proc = subprocess.run(
                [
                    str(install_script),
                    "--version",
                    tag,
                    "--install-dir",
                    str(dest_ok),
                    "--bin",
                    "all",
                    "--base-url",
                    (tmp_path / "releases").as_uri(),
                ],
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(ok_proc.returncode, 0, ok_proc.stderr)

            for name in ("simraft-node", "simraft-sim"):
                installed = dest_ok / name
                self.assertTrue(installed.is_file())
                self.assertEqual(installed.stat().st_mode & 0o777, 0o755)
                ver_proc = subprocess.run(
                    [str(installed), "--version"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(ver_proc.returncode, 0, ver_proc.stderr)
                self.assertIn("0.2.2", ver_proc.stdout)

            # Tamper with the second archive's SHA-256 and ensure install.sh
            # fails closed (exit 1) BEFORE extracting even the first binary.
            tampered_first_char = (
                "0" if sums_lines[1][0] != "0" else "1"
            )
            tampered_sums = [
                sums_lines[0],
                tampered_first_char + sums_lines[1][1:],
            ]
            sums_file.write_text(
                "\n".join(tampered_sums) + "\n", encoding="utf-8"
            )

            dest_bad = tmp_path / "bin-tampered"
            bad_proc = subprocess.run(
                [
                    str(install_script),
                    "--version",
                    tag,
                    "--install-dir",
                    str(dest_bad),
                    "--bin",
                    "all",
                    "--base-url",
                    (tmp_path / "releases").as_uri(),
                ],
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(bad_proc.returncode, 1)
            self.assertIn("checksum mismatch", bad_proc.stderr)
            self.assertFalse(
                (dest_bad / "simraft-node").exists(),
                "must fail closed before extracting any archive",
            )
            self.assertFalse((dest_bad / "simraft-sim").exists())

    def test_chaos_sweep_is_deterministic_and_linearizable(self) -> None:
        first = simraft_kv.simulate(seed=912, seeds=3, chaos=True)
        second = simraft_kv.simulate(seed=912, seeds=3, chaos=True)
        self.assertTrue(first.all_passed)
        self.assertEqual(first, second)
        self.assertEqual(len(first.runs), 3)
        for run in first.runs:
            self.assertEqual(run.verdict, "linearizable")
            self.assertTrue(run.converged)
            self.assertTrue(run.leader_converged)
            self.assertEqual(len(run.digest), 64)

    def test_replay_trace_emits_summary_and_events(self) -> None:
        report, trace = simraft_kv.replay_trace(seed=912, chaos=True)
        self.assertEqual(report.seed, 912)
        self.assertTrue(report.passed)
        self.assertGreater(len(trace), 100)

    def test_srcl_request_encoding_layout_and_bounds(self) -> None:
        put_frame = simraft_kv.encode_client_request(7, 3, "put", "k1", "v1")
        expected_put_body = (
            b"SRCL"
            + struct.pack("<HBQQI", 1, 1, 7, 3, 2)
            + b"k1"
            + struct.pack("<I", 2)
            + b"v1"
        )
        self.assertEqual(
            put_frame,
            struct.pack("<I", len(expected_put_body)) + expected_put_body,
        )

        get_frame = simraft_kv.encode_client_request(7, 0, "get", b"\x00key")
        expected_get_body = (
            b"SRCL" + struct.pack("<HBQQI", 1, 2, 7, 0, 4) + b"\x00key"
        )
        self.assertEqual(
            get_frame,
            struct.pack("<I", len(expected_get_body)) + expected_get_body,
        )

        del_frame = simraft_kv.encode_client_request(9, 4, "delete", "gone")
        expected_del_body = (
            b"SRCL" + struct.pack("<HBQQI", 1, 3, 9, 4, 4) + b"gone"
        )
        self.assertEqual(
            del_frame,
            struct.pack("<I", len(expected_del_body)) + expected_del_body,
        )

        with self.assertRaises(ValueError):
            simraft_kv.encode_client_request(1, 1, "put", "k", None)
        with self.assertRaises(ValueError):
            simraft_kv.encode_client_request(1, 0, "get", "k", "unexpected")
        with self.assertRaises(ValueError):
            simraft_kv.encode_client_request(1, 1, "delete", "k", "unexpected")
        with self.assertRaises(ValueError):
            simraft_kv.encode_client_request(1, 1, "cas", "k", "v")
        with self.assertRaises(ValueError):
            simraft_kv.encode_client_request(-1, 1, "get", "k")
        with self.assertRaises(ValueError):
            simraft_kv.encode_client_request(
                1, 1, "put", b"k", bytes(simraft_kv.MAX_PAYLOAD_BYTES)
            )
        with self.assertRaises(ValueError):
            simraft_kv.encode_client_request(
                1, 0, "get", bytes(simraft_kv.MAX_PAYLOAD_BYTES + 1)
            )
        with self.assertRaises(ValueError):
            simraft_kv.encode_client_request(
                1, 1, "delete", bytes(simraft_kv.MAX_PAYLOAD_BYTES - 20)
            )

        client = simraft_kv.SimRaftClient(
            ["127.0.0.1:9"], client_id=42, timeout=0.0
        )
        with self.assertRaises(ValueError):
            client.put(b"k", bytes(simraft_kv.MAX_PAYLOAD_BYTES))
        self.assertEqual(
            client._next_seq,
            0,
            "local payload rejection must not burn a session sequence number",
        )

    def test_srcl_response_decoding_and_malformed_rejection(self) -> None:
        ok_val = simraft_kv.decode_client_response(
            b"SRCL" + struct.pack("<HBBI", 1, 0, 1, 5) + b"hello"
        )
        self.assertEqual(
            ok_val, simraft_kv.ClientReply(status="ok", value=b"hello")
        )

        ok_none = simraft_kv.decode_client_response(
            b"SRCL" + struct.pack("<HBBI", 1, 0, 0, 0)
        )
        self.assertEqual(
            ok_none, simraft_kv.ClientReply(status="ok", value=None)
        )

        redir = simraft_kv.decode_client_response(
            b"SRCL" + struct.pack("<HBBI", 1, 1, 0, 14) + b"127.0.0.1:7002"
        )
        self.assertEqual(
            redir,
            simraft_kv.ClientReply(
                status="redirect", leader_hint="127.0.0.1:7002"
            ),
        )

        redir_unknown = simraft_kv.decode_client_response(
            b"SRCL" + struct.pack("<HBBI", 1, 1, 0, 0)
        )
        self.assertEqual(
            redir_unknown,
            simraft_kv.ClientReply(status="redirect", leader_hint=None),
        )

        err_reply = simraft_kv.decode_client_response(
            b"SRCL" + struct.pack("<HBBI", 1, 2, 0, 4) + b"busy"
        )
        self.assertEqual(
            err_reply, simraft_kv.ClientReply(status="error", error="busy")
        )

        bad_bodies = [
            b"SRCL\x01\x00",  # truncated header
            b"NOPE" + struct.pack("<HBBI", 1, 0, 0, 0),  # bad magic
            b"SRCL" + struct.pack("<HBBI", 2, 0, 0, 0),  # bad version
            b"SRCL" + struct.pack("<HBBI", 1, 0, 2, 0),  # bad present byte
            b"SRCL" + struct.pack("<HBBI", 1, 0, 1, 5) + b"hi",  # truncated data
            b"SRCL" + struct.pack("<HBBI", 1, 0, 0, 0) + b"x",  # trailing byte
            b"SRCL" + struct.pack("<HBBI", 1, 1, 0, 7) + b"badaddr",  # bad redirect
            b"SRCL" + struct.pack("<HBBI", 1, 9, 0, 0),  # unknown status
        ]
        for bad in bad_bodies:
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    simraft_kv.decode_client_response(bad)

    def test_find_node_binary_overrides(self) -> None:
        with self.assertRaises(FileNotFoundError):
            simraft_kv.find_node_binary("/nonexistent/path/to/simraft-node")

        prev = os.environ.get("SIMRAFT_NODE_BIN")
        try:
            os.environ["SIMRAFT_NODE_BIN"] = "/nonexistent/env/simraft-node"
            with self.assertRaises(FileNotFoundError):
                simraft_kv.find_node_binary()
        finally:
            if prev is None:
                os.environ.pop("SIMRAFT_NODE_BIN", None)
            else:
                os.environ["SIMRAFT_NODE_BIN"] = prev

    def test_stream_redirect_leader_promotion_and_session_dedup(self) -> None:
        follower_hits = 0
        observed_writes: list[tuple[int, int, int, bytes, bytes | None]] = []
        applied_mutations = 0
        sessions: dict[int, int] = {}
        store: dict[bytes, bytes] = {}
        dropped_first_put = False
        workers: list[threading.Thread] = []

        def fake_create_connection(
            address: tuple[str, int], timeout: float | None = None
        ) -> socket.socket:
            del timeout
            target = f"{address[0]}:{address[1]}"
            if target == "127.0.0.1:7001":
                raise ConnectionRefusedError("node 1 offline")
            client_sock, server_sock = socket.socketpair()

            def serve_one() -> None:
                nonlocal follower_hits, applied_mutations, dropped_first_put
                with server_sock:
                    op, cid, seq, key, val = _decode_wire_request(server_sock)
                    if target == "127.0.0.1:7002":
                        follower_hits += 1
                        server_sock.sendall(
                            _frame_response(1, 0, b"127.0.0.1:7003")
                        )
                        return
                    if op in (1, 3):
                        observed_writes.append((op, cid, seq, key, val))
                        if sessions.get(cid, 0) < seq:
                            sessions[cid] = seq
                            applied_mutations += 1
                            if op == 1 and val is not None:
                                store[key] = val
                            elif op == 3:
                                store.pop(key, None)
                        if op == 1 and not dropped_first_put:
                            dropped_first_put = True
                            return
                        server_sock.sendall(_frame_response(0, 0, b""))
                    elif op == 2:
                        found = store.get(key)
                        if found is None:
                            server_sock.sendall(_frame_response(0, 0, b""))
                        else:
                            server_sock.sendall(_frame_response(0, 1, found))

            worker = threading.Thread(target=serve_one, daemon=True)
            worker.start()
            workers.append(worker)
            return client_sock

        with unittest.mock.patch.object(
            simraft_kv.socket,
            "create_connection",
            side_effect=fake_create_connection,
        ):
            client = simraft_kv.SimRaftClient(
                ["127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"],
                client_id=0xBEEF_0007,
                timeout=5.0,
                connect_timeout=0.5,
            )
            client.put("counter", "once")
            self.assertEqual(client.servers[0], "127.0.0.1:7003")
            self.assertEqual(follower_hits, 2)
            hits_after_put = follower_hits
            self.assertEqual(len(observed_writes), 2)
            self.assertEqual(
                observed_writes[0], (1, 0xBEEF_0007, 1, b"counter", b"once")
            )
            self.assertEqual(
                observed_writes[1],
                observed_writes[0],
                "retry of put must preserve identical (client_id, seq)",
            )
            self.assertEqual(applied_mutations, 1)
            self.assertEqual(client.get_text("counter"), "once")
            self.assertIsNone(client.get("absent"))
            client.delete("counter")
            self.assertIsNone(client.get("counter"))
            self.assertEqual(
                follower_hits,
                hits_after_put,
                "promoted leader should serve subsequent calls without extra redirects",
            )
            self.assertEqual(
                observed_writes[-1], (3, 0xBEEF_0007, 2, b"counter", None)
            )

        for worker in workers:
            worker.join(timeout=2.0)

    @unittest.skipIf(
        _loopback_tcp_blocked(),
        "loopback TCP socket connect/bind is blocked by sandbox",
    )
    def test_mock_tcp_redirect_leader_promotion_and_session_dedup(
        self,
    ) -> None:
        follower_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        follower_sock.bind(("127.0.0.1", 0))
        follower_sock.listen(8)
        follower_addr = f"127.0.0.1:{follower_sock.getsockname()[1]}"

        leader_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        leader_sock.bind(("127.0.0.1", 0))
        leader_sock.listen(16)
        leader_addr = f"127.0.0.1:{leader_sock.getsockname()[1]}"

        stop_event = threading.Event()
        follower_hits = 0
        observed_writes: list[tuple[int, int, int, bytes, bytes | None]] = []
        applied_mutations = 0
        sessions: dict[int, int] = {}
        store: dict[bytes, bytes] = {}
        dropped_first_put = False

        def run_follower() -> None:
            nonlocal follower_hits
            follower_sock.settimeout(0.1)
            while not stop_event.is_set():
                try:
                    conn, _ = follower_sock.accept()
                except TimeoutError:
                    continue
                except OSError:
                    break
                with conn:
                    follower_hits += 1
                    _decode_wire_request(conn)
                    conn.sendall(
                        _frame_response(1, 0, leader_addr.encode("utf-8"))
                    )

        def run_leader() -> None:
            nonlocal applied_mutations, dropped_first_put
            leader_sock.settimeout(0.1)
            while not stop_event.is_set():
                try:
                    conn, _ = leader_sock.accept()
                except TimeoutError:
                    continue
                except OSError:
                    break
                with conn:
                    op, cid, seq, key, val = _decode_wire_request(conn)
                    if op in (1, 3):
                        observed_writes.append((op, cid, seq, key, val))
                        if sessions.get(cid, 0) < seq:
                            sessions[cid] = seq
                            applied_mutations += 1
                            if op == 1 and val is not None:
                                store[key] = val
                            elif op == 3:
                                store.pop(key, None)
                        if op == 1 and not dropped_first_put:
                            dropped_first_put = True
                            # Simulate connection drop right after leader commits
                            # so client must retry with identical (client_id, seq).
                            continue
                        conn.sendall(_frame_response(0, 0, b""))
                    elif op == 2:
                        found = store.get(key)
                        if found is None:
                            conn.sendall(_frame_response(0, 0, b""))
                        else:
                            conn.sendall(_frame_response(0, 1, found))

        t_follower = threading.Thread(target=run_follower, daemon=True)
        t_leader = threading.Thread(target=run_leader, daemon=True)
        t_follower.start()
        t_leader.start()
        try:
            client = simraft_kv.SimRaftClient(
                [follower_addr, leader_addr],
                client_id=0xCAFE_0001,
                timeout=5.0,
                connect_timeout=0.5,
            )
            client.put("counter", "once")
            self.assertEqual(client.servers[0], leader_addr)
            self.assertEqual(follower_hits, 2)
            hits_after_put = follower_hits
            self.assertEqual(len(observed_writes), 2)
            self.assertEqual(
                observed_writes[0], (1, 0xCAFE_0001, 1, b"counter", b"once")
            )
            self.assertEqual(
                observed_writes[1],
                observed_writes[0],
                "retry of put must preserve identical (client_id, seq)",
            )
            self.assertEqual(
                applied_mutations,
                1,
                "session deduplication must apply retried write only once",
            )

            self.assertEqual(client.get_text("counter"), "once")
            self.assertIsNone(client.get("absent"))
            client.delete("counter")
            self.assertIsNone(client.get("counter"))
            self.assertEqual(
                follower_hits,
                hits_after_put,
                "promoted leader should serve subsequent calls without extra redirects",
            )
            self.assertEqual(observed_writes[-1], (3, 0xCAFE_0001, 2, b"counter", None))
        finally:
            stop_event.set()
            follower_sock.close()
            leader_sock.close()
            t_follower.join(timeout=2.0)
            t_leader.join(timeout=2.0)

    @unittest.skipIf(
        _loopback_tcp_blocked(),
        "loopback TCP socket connect/bind is blocked by sandbox",
    )
    @unittest.skipUnless(
        _has_node_binary(),
        "simraft-node binary is not built or installed",
    )
    def test_live_three_node_simraft_node_cluster(self) -> None:
        node_bin = simraft_kv.find_node_binary()
        ports = _reserve_loopback_ports(3)
        addrs = [f"127.0.0.1:{p}" for p in ports]
        member_flags: list[str] = []
        for idx, addr in enumerate(addrs, start=1):
            member_flags.extend(["--member", f"{idx}@{addr}"])

        procs: dict[str, subprocess.Popen[bytes]] = {}
        with tempfile.TemporaryDirectory(prefix="simraft-py-live-") as tmp:
            try:
                for idx, addr in enumerate(addrs, start=1):
                    data_dir = Path(tmp) / f"node-{idx}"
                    data_dir.mkdir(parents=True, exist_ok=True)
                    cmd = [
                        str(node_bin),
                        "serve",
                        "--id",
                        str(idx),
                        "--data-dir",
                        str(data_dir),
                        "--tick-ms",
                        "25",
                        *member_flags,
                    ]
                    procs[addr] = subprocess.Popen(
                        cmd,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE,
                    )

                client = simraft_kv.SimRaftClient(
                    addrs, timeout=15.0, connect_timeout=1.0
                )
                client.put("greeting", "hello-raft")
                client.put(b"raw-bytes", b"\x00\xff\x42")

                self.assertEqual(client.get_text("greeting"), "hello-raft")
                self.assertEqual(client.get(b"raw-bytes"), b"\x00\xff\x42")
                self.assertIsNone(client.get("missing-key"))

                # Kill the promoted leader and verify failover across the
                # surviving 2-node quorum while preserving linearizability.
                leader_addr = client.servers[0]
                leader_proc = procs.pop(leader_addr)
                leader_proc.terminate()
                leader_proc.wait(timeout=5.0)
                if leader_proc.stderr is not None:
                    leader_proc.stderr.close()

                self.assertEqual(client.get_text("greeting"), "hello-raft")
                client.put("after-failover", "committed")
                client.delete("greeting")
                self.assertIsNone(client.get("greeting"))
                self.assertEqual(client.get_text("after-failover"), "committed")
                self.assertNotEqual(client.servers[0], leader_addr)
            finally:
                for proc in procs.values():
                    proc.terminate()
                for proc in procs.values():
                    try:
                        proc.wait(timeout=5.0)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=5.0)
                    finally:
                        if proc.stderr is not None:
                            proc.stderr.close()


if __name__ == "__main__":
    unittest.main()
