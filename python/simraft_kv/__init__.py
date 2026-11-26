"""Typed Python SDK and deterministic chaos simulator wrapper for ``simraft-kv``."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import sysconfig

__all__ = [
    "__version__",
    "SeedReport",
    "SimRaftError",
    "SweepSummary",
    "find_binary",
    "replay_trace",
    "simulate",
    "version",
]

__version__ = "0.2.0"

PathInput = str | os.PathLike[str]


class SimRaftError(RuntimeError):
    """Raised when ``simraft-sim`` exits nonzero or fails an invariant check."""

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


def find_binary(binary: PathInput | None = None) -> Path:
    """Locate the bundled or installed ``simraft-sim`` executable."""
    if binary is not None:
        candidate = Path(binary)
        if candidate.is_file():
            return candidate
        raise FileNotFoundError(f"simraft-sim binary not found at {candidate}")

    env_override = os.environ.get("SIMRAFT_SIM_BIN", "").strip()
    if env_override:
        candidate = Path(env_override)
        if candidate.is_file():
            return candidate
        raise FileNotFoundError(
            f"SIMRAFT_SIM_BIN points to missing file {candidate}"
        )

    exe_names = (
        ("simraft-sim.exe", "simraft-sim")
        if os.name == "nt"
        else ("simraft-sim",)
    )
    search_dirs: list[Path] = []
    scripts_dir = sysconfig.get_path("scripts")
    if scripts_dir:
        search_dirs.append(Path(scripts_dir))
    py_dir = Path(sys.executable).resolve().parent
    search_dirs.extend([py_dir, py_dir / "Scripts", py_dir / "bin"])

    repo_root = Path(__file__).resolve().parents[2]
    search_dirs.extend(
        [
            repo_root / "target" / "release",
            repo_root / "target" / "debug",
        ]
    )

    for directory in search_dirs:
        for name in exe_names:
            candidate = directory / name
            if candidate.is_file():
                return candidate

    found = shutil.which("simraft-sim")
    if found:
        return Path(found)

    raise FileNotFoundError(
        "could not locate simraft-sim; install simraft-kv or set SIMRAFT_SIM_BIN"
    )


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
