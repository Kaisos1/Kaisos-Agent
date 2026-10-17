"""Guards that keep unattended automation off the GPU and the job list sane.

Run: python3 -m unittest discover -s tests   (no dependencies, no network,
no model — every LLM call is replaced; the workspace is a temp dir)
"""
import importlib.machinery
import json
import os
import pathlib
import sys
import tempfile
import time
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent


def load_agent():
    sys.argv = ["kaisos.py"]
    return importlib.machinery.SourceFileLoader("kaisos_agent", str(ROOT / "kaisos.py")).load_module()


k = load_agent()


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ws = pathlib.Path(self.tmp.name)
        self.flag = self.ws / "gaming_mode"
        self._saved = (k.WORKSPACE, k.GAMING_FLAG, k.GAMING_GRACE_SEC, k._gaming_seen,
                       k.run_background, k.run_watches, k.process_inbox, k.run_heartbeat)
        k.WORKSPACE = self.ws
        k.GAMING_FLAG = str(self.flag)
        k.GAMING_GRACE_SEC = 0.3
        k._gaming_seen = 0.0
        self.ran = []
        k.run_background = lambda task, tag, notify=None: (self.ran.append(tag), ("ok", True))[1]

    def tearDown(self):
        (k.WORKSPACE, k.GAMING_FLAG, k.GAMING_GRACE_SEC, k._gaming_seen,
         k.run_background, k.run_watches, k.process_inbox, k.run_heartbeat) = self._saved
        self.tmp.cleanup()

    def due_job(self, jid):
        return {"id": jid, "kind": "schedule", "task": f"task {jid}", "when": "every 2 hours",
                "recurring": True, "interval": 7200, "daily": None, "next": time.time() - 60}


class GamingHold(Base):
    def test_never_seen_is_free(self):
        self.assertFalse(k.gaming_hold())

    def test_holds_while_flag_and_for_the_grace_after(self):
        self.flag.touch()
        self.assertTrue(k.gaming_hold())
        self.flag.unlink()
        self.assertTrue(k.gaming_hold(), "a 12 s Steam restart gap must not release the hold")
        time.sleep(0.35)
        self.assertFalse(k.gaming_hold())

    def test_empty_flag_setting_disables(self):
        k.GAMING_FLAG = ""
        self.flag.touch()
        self.assertFalse(k.gaming_hold())

    def test_tick_skips_every_step_while_held(self):
        steps = []
        k.run_watches = lambda n=None: steps.append("watches")
        k.process_inbox = lambda n=None: steps.append("inbox")
        k.run_heartbeat = lambda n=None: steps.append("heartbeat")
        self.flag.touch()
        k.daemon_tick()
        self.assertEqual(steps, [])
        self.flag.unlink()
        time.sleep(0.35)
        k.daemon_tick()
        self.assertEqual(steps, ["watches", "inbox", "heartbeat"])

    def test_batch_stops_when_a_game_starts(self):
        k.jobs_path().write_text(json.dumps([self.due_job("a"), self.due_job("b")]))
        def first_then_game(task, tag, notify=None):
            self.ran.append(tag)
            self.flag.touch()          # the game starts while job "a" runs
            return "ok", True
        k.run_background = first_then_game
        k.run_due_jobs()
        self.assertEqual(self.ran, ["a"])


class ScheduleGuard(Base):
    def test_identical_task_is_not_added_twice(self):
        k.t_schedule_task("every day at 09:00", "check  the disk")
        out = k.t_schedule_task("every day at 09:00", "check the disk")
        self.assertIn("Already scheduled", out)
        self.assertEqual(len(json.loads(k.jobs_path().read_text())), 1)

    def test_a_tool_name_is_not_a_task(self):
        with self.assertRaises(k.ToolError):
            k.t_schedule_task("every day at 09:00", "run_job arch_disk_alert")

    def test_disabled_duplicate_does_not_block(self):
        k.t_schedule_task("every day at 09:00", "check the disk")
        jobs = json.loads(k.jobs_path().read_text())
        jobs[0]["disabled"] = True
        k.jobs_path().write_text(json.dumps(jobs))
        self.assertIn("Scheduled", k.t_schedule_task("every day at 09:00", "check the disk"))


class ReplyCap(unittest.TestCase):
    def test_replies_are_bounded_by_default(self):
        if "AGENT_OLLAMA_NUM_PREDICT" in os.environ:
            self.skipTest("overridden in the environment")
        self.assertGreater(k.OLLAMA_NUM_PREDICT, 0)
        self.assertEqual(k._ollama_options().get("num_predict"), k.OLLAMA_NUM_PREDICT)


if __name__ == "__main__":
    unittest.main()
