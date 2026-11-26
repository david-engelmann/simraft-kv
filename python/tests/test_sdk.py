from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
if importlib.util.find_spec("simraft_kv") is None:
    sys.path.insert(0, str(ROOT / "python"))

import simraft_kv  # noqa: E402


class SimRaftKvSdkTests(unittest.TestCase):
    def test_version_matches_package(self) -> None:
        self.assertEqual(simraft_kv.version(), simraft_kv.__version__)

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


if __name__ == "__main__":
    unittest.main()
