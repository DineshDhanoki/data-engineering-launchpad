import contextlib
import io
import json
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock

from launchpad import preflight


def result(data=None, *, code=0, stderr=""):
    return subprocess.CompletedProcess([], code, "" if data is None else json.dumps(data), stderr)


class PreflightTests(unittest.TestCase):
    def run_preflight(self, responses):
        stdout = io.StringIO()
        stderr = io.StringIO()
        seen_databases = []

        def simulate(command, **options):
            if "--db" in command:
                database = Path(command[command.index("--db") + 1])
                database.write_text("fixture", encoding="utf-8")
                seen_databases.append(database)
            return next(responses)

        with (
            mock.patch.object(preflight.subprocess, "run", side_effect=simulate) as run,
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            code = preflight.main([])
        for database in seen_databases:
            self.assertFalse(database.parent.exists())
        return code, stdout.getvalue(), stderr.getvalue(), run.call_args_list, seen_databases

    def test_runs_public_commands_and_cleans_up_after_success(self):
        code, stdout, stderr, calls, databases = self.run_preflight(iter([
            result(), result(preflight.COUNTS), result(preflight.COUNTS), result(preflight.REVENUE),
            result(preflight.CHECK),
        ]))
        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        self.assertIn("Preflight passed.", stdout)
        self.assertEqual(calls[0].args[0], [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"])
        self.assertEqual(calls[1].args[0], calls[2].args[0])
        self.assertEqual(calls[1].args[0][2:6], ["launchpad.pipeline", "run", "--input", "labs/01-orders/orders.csv"])
        self.assertEqual(calls[3].args[0][2:5], ["launchpad.pipeline", "report", "--db"])
        self.assertEqual(calls[4].args[0][2:5], ["launchpad.pipeline", "check", "--db"])
        self.assertIn("--as-of", calls[4].args[0])
        self.assertTrue(all(call.kwargs["cwd"] == preflight.ROOT for call in calls))
        self.assertEqual(len(set(databases)), 1)

    def test_replay_mismatch_names_stage_and_cleans_up(self):
        code, _, stderr, calls, databases = self.run_preflight(iter([
            result(), result(preflight.COUNTS), result({**preflight.COUNTS, "current_orders": 4}),
        ]))
        self.assertEqual(code, 1)
        self.assertIn("FAIL Lab 01 replay", stderr)
        self.assertIn("expected", stderr)
        self.assertEqual(len(calls), 3)
        self.assertEqual(len(databases), 2)

    def test_subcommand_failure_reports_command_status_and_error(self):
        code, _, stderr, calls, _ = self.run_preflight(iter([
            result(), result(code=2, stderr="missing input"),
        ]))
        self.assertEqual(code, 2)
        self.assertIn("FAIL Lab 01 ingest (exit 2)", stderr)
        self.assertIn("launchpad.pipeline run", stderr)
        self.assertIn("missing input", stderr)
        self.assertEqual(len(calls), 2)
