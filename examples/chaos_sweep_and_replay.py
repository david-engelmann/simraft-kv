#!/usr/bin/env python3
"""Deterministic chaos sweep, byte-identical trace replay, and offline SRCL wire walkthrough."""

from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
import struct
import sys

ROOT = Path(__file__).resolve().parents[1]
if importlib.util.find_spec("simraft_kv") is None:
    sys.path.insert(0, str(ROOT / "python"))

import simraft_kv  # noqa: E402


def run_chaos_sweep() -> None:
    sweep = simraft_kv.simulate(seed=908, seeds=10, chaos=True)
    assert sweep.all_passed, f"expected all 10 chaos seeds to pass: {sweep}"
    assert len(sweep.runs) == 10
    for run in sweep.runs:
        assert run.completed and run.converged and run.leader_converged, run
        assert run.verdict == "linearizable", run
        assert len(run.digest) == 64, run
    print(
        f"[1/3] 10-seed chaos sweep passed "
        f"(seeds {sweep.runs[0].seed}..{sweep.runs[-1].seed}, all linearizable)"
    )


def verify_deterministic_replay() -> None:
    report_a, trace_a = simraft_kv.replay_trace(seed=912, chaos=True)
    report_b, trace_b = simraft_kv.replay_trace(seed=912, chaos=True)
    assert report_a.passed, report_a
    assert report_a == report_b, "SeedReport differed across identical seed replays"
    assert report_a.digest == report_b.digest, "state_digest differed across replays"
    assert trace_a == trace_b, "execution trace differed across replays"
    trace_sha = hashlib.sha256(trace_a.encode("utf-8")).hexdigest()[:16]
    print(
        f"[2/3] deterministic replay verified for seed {report_a.seed}: "
        f"ticks={report_a.ticks} state_digest={report_a.digest[:16]}... "
        f"trace_bytes={len(trace_a)} trace_sha256={trace_sha}..."
    )


def verify_srcl_wire_codec() -> None:
    put_req = simraft_kv.encode_client_request(
        client_id=42, seq=1, op="put", key="cluster/mode", value="quorum"
    )
    (put_body_len,) = struct.unpack_from("<I", put_req, 0)
    assert put_body_len == len(put_req) - 4
    assert put_req[4:8] == b"SRCL"

    get_req = simraft_kv.encode_client_request(
        client_id=42, seq=0, op="get", key="cluster/mode"
    )
    del_req = simraft_kv.encode_client_request(
        client_id=42, seq=2, op="delete", key="cluster/mode"
    )
    assert get_req[4:8] == b"SRCL" and del_req[4:8] == b"SRCL"

    ok_body = b"SRCL" + struct.pack("<HBBI", 1, 0, 1, 6) + b"quorum"
    ok_reply = simraft_kv.decode_client_response(ok_body)
    assert ok_reply.status == "ok" and ok_reply.value == b"quorum"

    redirect_body = (
        b"SRCL" + struct.pack("<HBBI", 1, 1, 0, 14) + b"127.0.0.1:7002"
    )
    redirect_reply = simraft_kv.decode_client_response(redirect_body)
    assert (
        redirect_reply.status == "redirect"
        and redirect_reply.leader_hint == "127.0.0.1:7002"
    )

    print(
        f"[3/3] SRCL v1 wire frames verified offline "
        f"(put={len(put_req)}B, get={len(get_req)}B, delete={len(del_req)}B)"
    )


def main() -> None:
    run_chaos_sweep()
    verify_deterministic_replay()
    verify_srcl_wire_codec()


if __name__ == "__main__":
    main()
