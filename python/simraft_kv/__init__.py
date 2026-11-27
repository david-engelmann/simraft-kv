"""Typed Python SDK, deterministic chaos simulator wrapper, and linearizable TCP client for ``simraft-kv``."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import sys
import sysconfig
import threading
import time

__all__ = [
    "__version__",
    "ClientReply",
    "MAX_CLIENT_BODY_BYTES",
    "MAX_PAYLOAD_BYTES",
    "SeedReport",
    "SimRaftClient",
    "SimRaftError",
    "SweepSummary",
    "completions",
    "decode_client_response",
    "encode_client_request",
    "find_binary",
    "find_node_binary",
    "replay_trace",
    "simulate",
    "version",
]

__version__ = "0.2.2"

PathInput = str | os.PathLike[str]

MAX_PAYLOAD_BYTES: int = 16 * 1024 * 1024
MAX_CLIENT_BODY_BYTES: int = MAX_PAYLOAD_BYTES + 64

_CLIENT_MAGIC = b"SRCL"
_CLIENT_VERSION = 1
_U64_MAX = 0xFFFF_FFFF_FFFF_FFFF

_OP_PUT = 1
_OP_GET = 2
_OP_DELETE = 3

_STATUS_OK = 0
_STATUS_REDIRECT = 1
_STATUS_ERROR = 2

_CLIENT_ID_LOCK = threading.Lock()
_PROCESS_SESSION_PREFIX: int | None = None
_NEXT_CLIENT_COUNTER = 0


def _splitmix64(x: int) -> int:
    x &= _U64_MAX
    x = ((x ^ (x >> 30)) * 0xBF58_476D_1CE4_E5B9) & _U64_MAX
    x = ((x ^ (x >> 27)) * 0x94D0_49BB_1331_11EB) & _U64_MAX
    return (x ^ (x >> 31)) & _U64_MAX


def _rotl64(x: int, k: int) -> int:
    x &= _U64_MAX
    return ((x << k) | (x >> (64 - k))) & _U64_MAX


def _next_client_id() -> int:
    """Allocate a collision-free 64-bit client session identifier."""
    global _PROCESS_SESSION_PREFIX, _NEXT_CLIENT_COUNTER
    with _CLIENT_ID_LOCK:
        if _PROCESS_SESSION_PREFIX is None:
            nanos = time.time_ns()
            lo = nanos & _U64_MAX
            hi = (nanos >> 64) & _U64_MAX
            seed = (((os.getpid() & 0xFFFF_FFFF) << 32) ^ lo ^ _rotl64(hi, 19)) & _U64_MAX
            _PROCESS_SESSION_PREFIX = _splitmix64(seed) & 0xFFFF_FFFF_0000_0000
        _NEXT_CLIENT_COUNTER += 1
        return (_PROCESS_SESSION_PREFIX + _NEXT_CLIENT_COUNTER) & _U64_MAX


class SimRaftError(RuntimeError):
    """Raised when ``simraft-sim`` or ``SimRaftClient`` fails an operation or invariant."""

    def __init__(self, message: str, *, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code


@dataclass(frozen=True, slots=True)
class SeedReport:
    """Single-seed simulation and linearizability verdict from ``simraft-sim``."""

    seed: int
    completed: bool
    converged: bool
    leader_converged: bool
    ticks: int
    verdict: str
    digest: str
    passed: bool


@dataclass(frozen=True, slots=True)
class SweepSummary:
    """Batch simulation summary across one or more consecutive seeds."""

    runs: tuple[SeedReport, ...]
    exit_code: int

    @property
    def all_passed(self) -> bool:
        """True when every seed in the sweep passed all safety and healing checks."""
        return self.exit_code == 0 and all(run.passed for run in self.runs)

    def raise_for_failures(self) -> None:
        """Raise ``SimRaftError`` when any seed failed."""
        if not self.all_passed:
            failed = [str(r.seed) for r in self.runs if not r.passed]
            detail = ", ".join(failed) if failed else "nonzero exit"
            raise SimRaftError(
                f"simraft-sim sweep failed on seed(s): {detail}",
                exit_code=self.exit_code or 1,
            )


@dataclass(frozen=True, slots=True)
class ClientReply:
    """Decoded ``"SRCL"`` v1 response from a ``simraft-node`` server."""

    status: str
    value: bytes | None = None
    leader_hint: str | None = None
    error: str | None = None


def _find_executable(
    binary: PathInput | None,
    *,
    bin_name: str,
    env_var: str,
    install_hint: str,
) -> Path:
    if binary is not None:
        candidate = Path(binary)
        if candidate.is_file():
            return candidate
        raise FileNotFoundError(f"{bin_name} binary not found at {candidate}")

    env_override = os.environ.get(env_var, "").strip()
    if env_override:
        candidate = Path(env_override)
        if candidate.is_file():
            return candidate
        raise FileNotFoundError(f"{env_var} points to missing file {candidate}")

    exe_names = (
        (f"{bin_name}.exe", bin_name) if os.name == "nt" else (bin_name,)
    )
    repo_root = Path(__file__).resolve().parents[2]
    cwd = Path.cwd()
    in_source_checkout = (repo_root / "Cargo.toml").is_file()

    target_candidates: list[Path] = []
    for base in (repo_root, cwd):
        for profile in ("release", "debug"):
            for name in exe_names:
                candidate = base / "target" / profile / name
                if candidate.is_file() and candidate not in target_candidates:
                    target_candidates.append(candidate)
    target_candidates.sort(key=lambda p: p.stat().st_mtime_ns, reverse=True)

    env_dirs: list[Path] = []
    scripts_dir = sysconfig.get_path("scripts")
    if scripts_dir:
        env_dirs.append(Path(scripts_dir))
    py_dir = Path(sys.executable).resolve().parent
    env_dirs.extend([py_dir, py_dir / "Scripts", py_dir / "bin"])
    env_candidates: list[Path] = []
    for directory in env_dirs:
        for name in exe_names:
            candidate = directory / name
            if candidate.is_file() and candidate not in env_candidates:
                env_candidates.append(candidate)

    ordered = (
        [*target_candidates, *env_candidates]
        if in_source_checkout
        else [*env_candidates, *target_candidates]
    )
    if ordered:
        return ordered[0]

    found = shutil.which(bin_name)
    if found:
        return Path(found)

    raise FileNotFoundError(
        f"could not locate {bin_name}; {install_hint} or set {env_var}"
    )


def find_binary(binary: PathInput | None = None) -> Path:
    """Locate the bundled or installed ``simraft-sim`` executable."""
    return _find_executable(
        binary,
        bin_name="simraft-sim",
        env_var="SIMRAFT_SIM_BIN",
        install_hint="install simraft-kv",
    )


def find_node_binary(binary: PathInput | None = None) -> Path:
    """Locate an installed or locally built ``simraft-node`` executable."""
    return _find_executable(
        binary,
        bin_name="simraft-node",
        env_var="SIMRAFT_NODE_BIN",
        install_hint="install simraft-node",
    )


def _coerce_bytes(data: bytes | str, field: str) -> bytes:
    if isinstance(data, str):
        return data.encode("utf-8")
    if isinstance(data, (bytes, bytearray, memoryview)):
        return bytes(data)
    raise TypeError(f"{field} must be bytes or str, got {type(data).__name__}")


def _validate_u64(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an int, got {type(value).__name__}")
    if not (0 <= value <= _U64_MAX):
        raise ValueError(f"{field} {value} out of u64 range 0..={_U64_MAX}")
    return value


def _parse_server_addr(spec: str) -> tuple[str, int]:
    if not isinstance(spec, str):
        raise TypeError(f"server address must be str, got {type(spec).__name__}")
    text = spec.strip()
    if not text:
        raise ValueError("server address must not be empty")
    if text.startswith("["):
        bracket_end = text.find("]")
        if (
            bracket_end <= 1
            or bracket_end + 1 >= len(text)
            or text[bracket_end + 1] != ":"
        ):
            raise ValueError(f"invalid bracketed server address {spec!r}")
        host = text[1:bracket_end]
        port_str = text[bracket_end + 2 :]
    else:
        if text.count(":") != 1:
            raise ValueError(
                f"invalid server address {spec!r}; expected 'host:port'"
            )
        host, port_str = text.rsplit(":", 1)
    if not host or not port_str:
        raise ValueError(f"invalid server address {spec!r}")
    try:
        port = int(port_str, 10)
    except ValueError as exc:
        raise ValueError(
            f"server address {spec!r} has non-integer port"
        ) from exc
    if not (0 <= port <= 65535):
        raise ValueError(
            f"server address {spec!r} port {port} out of range 0..65535"
        )
    return host, port


def encode_client_request(
    client_id: int,
    seq: int,
    op: str,
    key: bytes | str,
    value: bytes | str | None = None,
) -> bytes:
    """Encode a length-prefixed ``"SRCL"`` v1 client request matching ``simraft-node``."""
    cid = _validate_u64(client_id, "client_id")
    seq_num = _validate_u64(seq, "seq")
    if not isinstance(op, str):
        raise TypeError(f"op must be str, got {type(op).__name__}")
    op_norm = op.strip().lower()
    key_bytes = _coerce_bytes(key, "key")

    if op_norm == "put":
        if value is None:
            raise ValueError("put operation requires a non-None value")
        val_bytes = _coerce_bytes(value, "value")
        body_len = 31 + len(key_bytes) + len(val_bytes)
        if body_len > MAX_CLIENT_BODY_BYTES:
            raise ValueError(
                f"client request body length {body_len} bytes exceeds limit of {MAX_CLIENT_BODY_BYTES} bytes"
            )
        if len(key_bytes) + len(val_bytes) + 25 > MAX_PAYLOAD_BYTES:
            raise ValueError(
                f"client command payload exceeds limit of {MAX_PAYLOAD_BYTES} bytes"
            )
        return (
            struct.pack(
                "<I4sHBQQI",
                body_len,
                _CLIENT_MAGIC,
                _CLIENT_VERSION,
                _OP_PUT,
                cid,
                seq_num,
                len(key_bytes),
            )
            + key_bytes
            + struct.pack("<I", len(val_bytes))
            + val_bytes
        )

    if op_norm == "get":
        if value is not None:
            raise ValueError("get operation does not accept a value")
        body_len = 27 + len(key_bytes)
        if len(key_bytes) > MAX_PAYLOAD_BYTES:
            raise ValueError(
                f"client key length {len(key_bytes)} exceeds limit of {MAX_PAYLOAD_BYTES} bytes"
            )
        if body_len > MAX_CLIENT_BODY_BYTES:
            raise ValueError(
                f"client request body length {body_len} bytes exceeds limit of {MAX_CLIENT_BODY_BYTES} bytes"
            )
        return (
            struct.pack(
                "<I4sHBQQI",
                body_len,
                _CLIENT_MAGIC,
                _CLIENT_VERSION,
                _OP_GET,
                cid,
                seq_num,
                len(key_bytes),
            )
            + key_bytes
        )

    if op_norm == "delete":
        if value is not None:
            raise ValueError("delete operation does not accept a value")
        body_len = 27 + len(key_bytes)
        if body_len > MAX_CLIENT_BODY_BYTES:
            raise ValueError(
                f"client request body length {body_len} bytes exceeds limit of {MAX_CLIENT_BODY_BYTES} bytes"
            )
        if len(key_bytes) + 21 > MAX_PAYLOAD_BYTES:
            raise ValueError(
                f"client command payload exceeds limit of {MAX_PAYLOAD_BYTES} bytes"
            )
        return (
            struct.pack(
                "<I4sHBQQI",
                body_len,
                _CLIENT_MAGIC,
                _CLIENT_VERSION,
                _OP_DELETE,
                cid,
                seq_num,
                len(key_bytes),
            )
            + key_bytes
        )

    raise ValueError(
        f"unknown client op {op!r}; expected 'put', 'get', or 'delete'"
    )


def decode_client_response(body: bytes) -> ClientReply:
    """Decode an unprefixed ``"SRCL"`` v1 response body into a ``ClientReply``."""
    if not isinstance(body, (bytes, bytearray, memoryview)):
        raise TypeError(f"body must be bytes-like, got {type(body).__name__}")
    raw = bytes(body)
    if len(raw) > MAX_CLIENT_BODY_BYTES:
        raise ValueError(
            f"client response body length {len(raw)} bytes exceeds limit of {MAX_CLIENT_BODY_BYTES} bytes"
        )
    if len(raw) < 12 or raw[:4] != _CLIENT_MAGIC:
        raise ValueError(
            f"client response malformed: length {len(raw)}, magic {raw[:4]!r}"
        )
    _, ver, status, present, data_len = struct.unpack("<4sHBBI", raw[:12])
    if ver != _CLIENT_VERSION:
        raise ValueError(
            f"client response version {ver}, expected {_CLIENT_VERSION}"
        )
    if present not in (0, 1):
        raise ValueError(f"client response invalid present flag {present}")
    if data_len > MAX_PAYLOAD_BYTES:
        raise ValueError(
            f"client response payload length {data_len} bytes exceeds limit of {MAX_PAYLOAD_BYTES} bytes"
        )
    if len(raw) != 12 + data_len:
        raise ValueError(
            f"client response length mismatch: header claims {data_len} data bytes, {len(raw) - 12} present"
        )
    data = raw[12:]
    if status == _STATUS_OK:
        return ClientReply(status="ok", value=data if present == 1 else None)
    if status == _STATUS_REDIRECT:
        if not data:
            return ClientReply(status="redirect", leader_hint=None)
        hint = data.decode("utf-8", errors="replace")
        _parse_server_addr(hint)
        return ClientReply(status="redirect", leader_hint=hint)
    if status == _STATUS_ERROR:
        return ClientReply(
            status="error", error=data.decode("utf-8", errors="replace")
        )
    raise ValueError(f"unknown client response status {status}")


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError(
                f"unexpected EOF reading SRCL frame ({len(buf)}/{n} bytes read)"
            )
        buf.extend(chunk)
    return bytes(buf)


def _send_srcl_request(
    server: str,
    request: bytes,
    *,
    connect_timeout: float,
    io_timeout: float,
) -> ClientReply:
    host, port = _parse_server_addr(server)
    with socket.create_connection((host, port), timeout=connect_timeout) as sock:
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass
        sock.settimeout(io_timeout)
        sock.sendall(request)
        prefix = _recv_exact(sock, 4)
        (body_len,) = struct.unpack("<I", prefix)
        if body_len > MAX_CLIENT_BODY_BYTES:
            raise ValueError(
                f"message length {body_len} bytes exceeds limit of {MAX_CLIENT_BODY_BYTES} bytes"
            )
        body = _recv_exact(sock, body_len)
    return decode_client_response(body)


class SimRaftClient:
    """Stateful linearizable TCP client session against a ``simraft-node`` cluster.

    Preserves a single ``client_id`` across calls and advances ``seq`` strictly
    monotonically on writes (``put`` and ``delete``) while reusing the exact
    same ``(client_id, seq)`` across retries of an in-flight write, guaranteeing
    linearizable at-most-once mutation via ``simraft-kv`` session deduplication.
    """

    def __init__(
        self,
        servers: Sequence[str],
        *,
        client_id: int | None = None,
        timeout: float = 10.0,
        connect_timeout: float = 1.0,
    ) -> None:
        if isinstance(servers, (str, bytes)) or not servers:
            raise ValueError(
                "servers must be a non-empty sequence of 'host:port' addresses"
            )
        deduped: list[str] = []
        for entry in servers:
            _parse_server_addr(entry)
            cleaned = entry.strip()
            if cleaned not in deduped:
                deduped.append(cleaned)
        if timeout < 0:
            raise ValueError(f"timeout must be >= 0, got {timeout}")
        if connect_timeout <= 0:
            raise ValueError(
                f"connect_timeout must be > 0, got {connect_timeout}"
            )
        self._servers = deduped
        self._client_id = (
            _next_client_id()
            if client_id is None
            else _validate_u64(client_id, "client_id")
        )
        self._next_seq = 0
        self._timeout = float(timeout)
        self._connect_timeout = float(connect_timeout)

    @property
    def client_id(self) -> int:
        """Return this session's 64-bit client identifier."""
        return self._client_id

    @property
    def servers(self) -> tuple[str, ...]:
        """Return candidate cluster addresses with the latest leader first."""
        return tuple(self._servers)

    def _promote_leader(self, addr: str) -> None:
        cleaned = addr.strip()
        if cleaned in self._servers:
            self._servers.remove(cleaned)
        self._servers.insert(0, cleaned)

    def _next_candidate_after(self, failed: str) -> str:
        if len(self._servers) <= 1:
            return self._servers[0]
        try:
            idx = self._servers.index(failed)
        except ValueError:
            return self._servers[0]
        return self._servers[(idx + 1) % len(self._servers)]

    def _execute(
        self,
        op: str,
        key: bytes | str,
        value: bytes | str | None = None,
    ) -> bytes | None:
        is_write = op in ("put", "delete")
        seq = self._next_seq + 1 if is_write else 0
        request = encode_client_request(self._client_id, seq, op, key, value)
        if is_write:
            self._next_seq = seq

        deadline = time.monotonic() + self._timeout
        target = self._servers[0]
        redirects_in_row = 0
        tried_in_round = 0
        last_detail = "no server contacted"

        while True:
            remaining = deadline - time.monotonic()
            if self._timeout == 0.0:
                conn_to = self._connect_timeout
                io_to = max(self._connect_timeout, 1.0)
            else:
                conn_to = max(0.02, min(self._connect_timeout, max(0.02, remaining)))
                io_to = max(
                    0.05,
                    min(max(self._connect_timeout * 2.0, 2.0), max(0.05, remaining)),
                )

            try:
                reply = _send_srcl_request(
                    target,
                    request,
                    connect_timeout=conn_to,
                    io_timeout=io_to,
                )
            except (OSError, ValueError) as exc:
                last_detail = f"{target}: {exc}"
                redirects_in_row = 0
                target = self._next_candidate_after(target)
                tried_in_round += 1
            else:
                if reply.status == "ok":
                    self._promote_leader(target)
                    return reply.value
                if reply.status == "redirect":
                    if reply.leader_hint and reply.leader_hint != target:
                        last_detail = (
                            f"redirected from {target} to {reply.leader_hint}"
                        )
                        self._promote_leader(reply.leader_hint)
                        target = reply.leader_hint
                        redirects_in_row += 1
                        if (
                            redirects_in_row <= len(self._servers)
                            and time.monotonic() < deadline
                        ):
                            continue
                        tried_in_round = len(self._servers)
                    else:
                        last_detail = f"{target} knows no leader yet"
                        redirects_in_row = 0
                        target = self._next_candidate_after(target)
                        tried_in_round += 1
                else:
                    last_detail = f"{target} reported: {reply.error}"
                    redirects_in_row = 0
                    target = self._next_candidate_after(target)
                    tried_in_round += 1

            now = time.monotonic()
            if now >= deadline:
                raise SimRaftError(
                    f"client {op} did not complete within {self._timeout:g}s: {last_detail}"
                )
            if tried_in_round < len(self._servers):
                continue
            tried_in_round = 0
            redirects_in_row = 0
            sleep_for = min(0.05, max(0.0, deadline - now))
            if sleep_for > 0.0:
                time.sleep(sleep_for)

    def put(self, key: bytes | str, value: bytes | str) -> None:
        """Linearizably store ``value`` under ``key``."""
        self._execute("put", key, value)

    def get(self, key: bytes | str) -> bytes | None:
        """Linearizably read ``key`` via ``ReadIndex``, returning ``None`` if absent."""
        return self._execute("get", key)

    def get_text(
        self, key: bytes | str, encoding: str = "utf-8"
    ) -> str | None:
        """Linearizably read ``key`` and decode as text, returning ``None`` if absent."""
        raw = self.get(key)
        return None if raw is None else raw.decode(encoding)

    def delete(self, key: bytes | str) -> None:
        """Linearizably remove ``key``."""
        self._execute("delete", key)


def version(*, binary: PathInput | None = None) -> str:
    """Return the version reported by ``simraft-sim --version``."""
    exe = find_binary(binary)
    proc = subprocess.run(
        [str(exe), "--version"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if proc.returncode != 0:
        raise SimRaftError(
            proc.stderr.strip() or "failed to query simraft-sim version",
            exit_code=proc.returncode,
        )
    parts = proc.stdout.strip().split()
    return parts[-1] if parts else proc.stdout.strip()


_SUPPORTED_SHELLS = ("bash", "zsh", "fish", "powershell")
_SUPPORTED_TOOLS = ("simraft-sim", "simraft-node")


def completions(
    shell: str = "bash",
    *,
    tool: str = "simraft-sim",
    binary: PathInput | None = None,
) -> str:
    """Return the deterministic shell completion script for ``tool`` in ``shell``."""
    if not isinstance(shell, str):
        raise TypeError(f"shell must be str, got {type(shell).__name__}")
    if not isinstance(tool, str):
        raise TypeError(f"tool must be str, got {type(tool).__name__}")
    shell_norm = shell.strip().lower()
    if shell_norm not in _SUPPORTED_SHELLS:
        raise ValueError(
            f"unsupported shell {shell!r}; expected 'bash', 'zsh', 'fish', or 'powershell'"
        )
    tool_norm = tool.strip()
    if tool_norm not in _SUPPORTED_TOOLS:
        raise ValueError(
            f"unsupported tool {tool!r}; expected 'simraft-sim' or 'simraft-node'"
        )
    if tool_norm == "simraft-sim":
        exe = find_binary(binary)
        cmd = [str(exe), "--completions", shell_norm]
    else:
        exe = find_node_binary(binary)
        cmd = [str(exe), "completions", shell_norm]

    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if proc.returncode != 0:
        raise SimRaftError(
            proc.stderr.strip()
            or f"failed to generate {tool_norm} {shell_norm} completions",
            exit_code=proc.returncode,
        )
    return proc.stdout


def _parse_seed_report(raw_json: str) -> SeedReport:
    data = json.loads(raw_json)
    return SeedReport(
        seed=int(data["seed"]),
        completed=bool(data["completed"]),
        converged=bool(data["converged"]),
        leader_converged=bool(data["leader_converged"]),
        ticks=int(data["ticks"]),
        verdict=str(data["verdict"]),
        digest=str(data["digest"]),
        passed=bool(data["passed"]),
    )


def simulate(
    *,
    seed: int = 0,
    seeds: int = 1,
    chaos: bool = False,
    nodes: int = 3,
    ticks: int = 8_000,
    check: bool = True,
    binary: PathInput | None = None,
) -> SweepSummary:
    """Run ``simraft-sim --json`` across ``seeds`` consecutive seeds."""
    exe = find_binary(binary)
    cmd = [
        str(exe),
        "--seed",
        str(seed),
        "--seeds",
        str(seeds),
        "--nodes",
        str(nodes),
        "--ticks",
        str(ticks),
        "--json",
    ]
    if chaos:
        cmd.append("--chaos")
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if proc.returncode not in (0, 1):
        raise SimRaftError(
            proc.stderr.strip() or "simraft-sim execution error",
            exit_code=proc.returncode,
        )
    lines = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
    if not lines and proc.returncode != 0:
        raise SimRaftError(
            proc.stderr.strip() or "simraft-sim produced no output",
            exit_code=proc.returncode,
        )
    runs = tuple(_parse_seed_report(line) for line in lines)
    summary = SweepSummary(runs=runs, exit_code=proc.returncode)
    if check:
        summary.raise_for_failures()
    return summary


def replay_trace(
    *,
    seed: int,
    chaos: bool = False,
    nodes: int = 3,
    ticks: int = 8_000,
    binary: PathInput | None = None,
) -> tuple[SeedReport, str]:
    """Run one seed with ``--json --trace`` and return ``(SeedReport, trace_text)``."""
    exe = find_binary(binary)
    cmd = [
        str(exe),
        "--seed",
        str(seed),
        "--seeds",
        "1",
        "--nodes",
        str(nodes),
        "--ticks",
        str(ticks),
        "--json",
        "--trace",
    ]
    if chaos:
        cmd.append("--chaos")
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    lines = proc.stdout.splitlines()
    if not lines:
        raise SimRaftError(
            proc.stderr.strip() or "simraft-sim produced no output",
            exit_code=proc.returncode or 1,
        )
    report = _parse_seed_report(lines[0])
    trace = "\n".join(lines[1:]) + ("\n" if len(lines) > 1 else "")
    return report, trace
