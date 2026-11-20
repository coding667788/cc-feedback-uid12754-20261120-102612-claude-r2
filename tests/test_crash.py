"""Crash-recovery tests.

The worker process is killed (it kills itself via os._exit, like an abrupt
termination) at two distinct points:

A. out-of-order messages saved inside the transaction but COMMIT not yet
   issued  (--crash OP:pre_commit)
B. queue advancement committed but the reply not yet written
   (--crash OP:post_commit)

After a restart the whole script is replayed; the tests verify ordered
delivery, stable rejection reasons, persistent receipts and the absence of
duplicates.
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest

from solution.core import Receiver, ERR

CRASH_EXIT = 3


def build_script(path):
    ops = [
        {"op": "establish", "session": "s", "tenant": "t1", "conn_gen": 1,
         "base_seq": 100},
        {"op": "key", "session": "s", "key_gen": 1, "lo": 100, "hi": 199},
        {"op": "recv", "session": "s", "seq": 102, "conn_gen": 1,
         "key_gen": 1, "digest": "d102"},                                  # 2
        {"op": "recv", "session": "s", "seq": 101, "conn_gen": 1,
         "key_gen": 1, "digest": "d101"},                                  # 3
        {"op": "recv", "session": "s", "seq": 100, "conn_gen": 1,
         "key_gen": 1, "digest": "d100"},                                  # 4
        {"op": "recv", "session": "s", "seq": 103, "conn_gen": 1,
         "key_gen": 1, "digest": "d103"},                                  # 5
        {"op": "recv", "session": "s", "seq": 104, "conn_gen": 1,
         "key_gen": 1, "digest": "d104"},                                  # 6
        {"op": "recv", "session": "s", "seq": 105, "conn_gen": 1,
         "key_gen": 1, "digest": "d105"},                                  # 7
        {"op": "recv", "session": "s", "seq": 350, "conn_gen": 1,
         "key_gen": 1, "digest": "d350"},                                  # 8
        {"op": "recv", "session": "s", "seq": 104, "conn_gen": 1,
         "key_gen": 1, "digest": "d104", "auth": False},                   # 9
        {"op": "recv", "session": "s", "seq": 105, "conn_gen": 1,
         "key_gen": 1, "digest": "d105"},                                  # 10
    ]
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"ops": ops}, fh)
    return ops


def run_worker(db, script, out, crash=None):
    cmd = [sys.executable, "-m", "solution.worker", "--db", db,
           "--script", script, "--out", out]
    if crash:
        cmd += ["--crash", crash]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=120)


def read_jsonl(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


class CrashTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="crash_test_")
        self.db = os.path.join(self.tmp, "recv.db")
        self.script = os.path.join(self.tmp, "script.json")
        self.ops = build_script(self.script)

    def assert_final_state_consistent(self):
        r = Receiver(self.db)
        try:
            delivered = r.delivered("s")
            abs_seqs = [d["abs_seq"] for d in delivered]
            # ordered, contiguous, duplicate-free delivery of 100..105
            self.assertEqual(abs_seqs, [100, 101, 102, 103, 104, 105])
            self.assertEqual(len(abs_seqs), len(set(abs_seqs)))
            self.assertEqual([d["digest"] for d in delivered],
                             [f"d{i}" for i in range(100, 106)])
            # persistent receipts: exactly one per accepted abs seq
            receipts = r.receipts("s")
            self.assertEqual(len(receipts), 6)
            self.assertEqual(len({x["abs_seq"] for x in receipts}), 6)
            self.assertTrue(all(x["state"] == "delivered" for x in receipts))
            # queue advancement is final
            self.assertEqual(r.session_state("s")["expected"], 106)
            # raw SQL double-check: no duplicate queue rows
            cur = r.conn.execute(
                "SELECT abs_seq, COUNT(*) c FROM receipts "
                "WHERE state='delivered' GROUP BY abs_seq HAVING c > 1")
            self.assertEqual(cur.fetchall(), [])
        finally:
            r.close()

    def assert_replay_rejections(self, replies):
        by_i = {r["i"]: r for r in replies}
        # rejection reasons are stable across the replay
        self.assertEqual(by_i[8]["code"], ERR["ERR_WINDOW_RIGHT"])
        self.assertEqual(by_i[8]["error"], "ERR_WINDOW_RIGHT")
        self.assertEqual(by_i[9]["code"], ERR["ERR_AUTH"])
        self.assertEqual(by_i[9]["error"], "ERR_AUTH")
        # retransmission of an already delivered message -> persistent receipt
        self.assertEqual(by_i[10]["status"], "duplicate")
        self.assertEqual(by_i[10]["receipt_state"], "delivered")
        self.assertEqual(by_i[10]["abs_seq"], 105)


class TestCrashPreCommit(CrashTestBase):
    """Killed after the out-of-order rows were saved but before COMMIT."""

    def test_restart_and_replay(self):
        out1 = os.path.join(self.tmp, "out1.jsonl")
        out2 = os.path.join(self.tmp, "out2.jsonl")

        # op 3 (recv 101) dies after INSERT, before COMMIT
        proc = run_worker(self.db, self.script, out1, crash="3:pre_commit")
        self.assertEqual(proc.returncode, CRASH_EXIT)
        replies1 = read_jsonl(out1)
        # only ops 0..2 produced replies; op 3 never committed
        self.assertEqual([r["i"] for r in replies1], [0, 1, 2])

        # the uncommitted message is gone; committed work survived
        r = Receiver(self.db)
        try:
            self.assertEqual([x["abs_seq"] for x in r.receipts("s")], [102])
            self.assertEqual(r.session_state("s")["expected"], 100)
        finally:
            r.close()

        # restart: replay the whole script against the same database
        proc = run_worker(self.db, self.script, out2)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        replies2 = read_jsonl(out2)
        self.assertEqual(len(replies2), len(self.ops))
        by_i = {r["i"]: r for r in replies2}

        # op 2 was committed before the crash -> duplicate on replay
        self.assertEqual(by_i[2]["status"], "duplicate")
        self.assertEqual(by_i[2]["receipt_state"], "cached")
        # op 3 rolled back -> accepted fresh on replay
        self.assertEqual(by_i[3]["status"], "cached")
        # ordered contiguous release once the predecessor arrives
        self.assertEqual(by_i[4]["status"], "delivered")
        self.assertEqual(by_i[4]["released"], [100, 101, 102])
        self.assertEqual(by_i[5]["released"], [103])
        self.assertEqual(by_i[6]["released"], [104])
        self.assertEqual(by_i[7]["released"], [105])

        self.assert_replay_rejections(replies2)
        self.assert_final_state_consistent()


class TestCrashPostCommit(CrashTestBase):
    """Killed after the queue advancement committed but before the reply."""

    def test_restart_and_replay(self):
        out1 = os.path.join(self.tmp, "out1.jsonl")
        out2 = os.path.join(self.tmp, "out2.jsonl")

        # op 4 (recv 100, triggers release of 100..102) commits, then dies
        # before its reply is written.
        proc = run_worker(self.db, self.script, out1, crash="4:post_commit")
        self.assertEqual(proc.returncode, CRASH_EXIT)
        replies1 = read_jsonl(out1)
        # replies for ops 0..3 only; op 4's reply was lost
        self.assertEqual([r["i"] for r in replies1], [0, 1, 2, 3])

        # ...but its queue advancement is durable
        r = Receiver(self.db)
        try:
            self.assertEqual(
                [d["abs_seq"] for d in r.delivered("s")], [100, 101, 102])
            self.assertEqual(r.session_state("s")["expected"], 103)
        finally:
            r.close()

        # restart: replay everything
        proc = run_worker(self.db, self.script, out2)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        replies2 = read_jsonl(out2)
        self.assertEqual(len(replies2), len(self.ops))
        by_i = {r["i"]: r for r in replies2}

        # ops 2..4 replay as duplicates answered from persistent receipts,
        # nothing is enqueued twice
        for i in (2, 3, 4):
            self.assertEqual(by_i[i]["status"], "duplicate")
            self.assertEqual(by_i[i]["receipt_state"], "delivered")
        self.assertEqual(by_i[5]["released"], [103])
        self.assertEqual(by_i[6]["released"], [104])
        self.assertEqual(by_i[7]["released"], [105])

        self.assert_replay_rejections(replies2)
        self.assert_final_state_consistent()


if __name__ == "__main__":
    unittest.main()
