"""Tests driven by independent writer processes (subprocesses), covering:

* the same absolute sequence enqueued from two keys simultaneously
* revocation after a missing predecessor
* the 65535 -> 0 wrap handoff
* reuse of an old connection number
* failed authentication never enqueues
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest

from solution.core import Receiver, ERR


def run_writer(db, *extra, expect=0):
    cmd = [sys.executable, "-m", "solution.writer", "--db", db, *extra]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    assert proc.returncode == expect, proc.stderr
    return [json.loads(line) for line in proc.stdout.splitlines()
            if line.strip()]


class WriterTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="writer_test_")
        self.db = os.path.join(self.tmp, "recv.db")

    def open(self):
        return Receiver(self.db)


class TestSameSeqTwoKeys(WriterTestBase):
    def test_only_one_writer_wins(self):
        r = self.open()
        r.establish("s", "t1", 1, 1000)
        r.add_key("s", 1, 1000, 1999)
        r.add_key("s", 2, 1000, 1999)  # overlapping: both keys valid
        r.close()

        common = ["--session", "s", "--conn-gen", "1", "--wire", "1100",
                  "--attempts", "15", "--delay-ms", "5"]
        pa = subprocess.Popen(
            [sys.executable, "-m", "solution.writer", "--db", self.db,
             *common, "--key-gen", "1", "--digest", "A"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        pb = subprocess.Popen(
            [sys.executable, "-m", "solution.writer", "--db", self.db,
             *common, "--key-gen", "2", "--digest", "B"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        out_a, err_a = pa.communicate(timeout=120)
        out_b, err_b = pb.communicate(timeout=120)
        self.assertEqual(pa.returncode, 0, err_a)
        self.assertEqual(pb.returncode, 0, err_b)

        lines_a = [json.loads(x) for x in out_a.splitlines() if x.strip()]
        lines_b = [json.loads(x) for x in out_b.splitlines() if x.strip()]
        self.assertEqual(len(lines_a), 15)
        self.assertEqual(len(lines_b), 15)

        def accepted(lines):
            return [x for x in lines if x["status"] in ("cached", "delivered")]

        # exactly one first-receipt across both processes
        self.assertEqual(len(accepted(lines_a)) + len(accepted(lines_b)), 1)
        winner, loser = (lines_a, lines_b) if accepted(lines_a) \
            else (lines_b, lines_a)
        # the winner's later attempts are duplicates of its own receipt
        for line in winner[1:]:
            self.assertEqual(line["status"], "duplicate")
        # the loser conflicts with the bound evidence on every attempt
        for line in loser:
            self.assertEqual(line["status"], "rejected")
            self.assertEqual(line["code"], ERR["ERR_CONFLICT"])

        r = self.open()
        try:
            rows = [x for x in r.receipts("s") if x["abs_seq"] == 1100]
            self.assertEqual(len(rows), 1)  # received at most once
            self.assertEqual(rows[0]["state"], "cached")
        finally:
            r.close()


class TestRevokeAfterMissingPredecessor(WriterTestBase):
    def test_scenario(self):
        r = self.open()
        r.establish("s", "t1", 1, 200)
        r.add_key("s", 1, 200, 299)
        r.add_key("s", 2, 200, 299)
        r.close()

        # independent writer caches 201 and 202 while 200 is missing
        out = run_writer(self.db, "--session", "s", "--conn-gen", "1",
                         "--wire", "201,202", "--key-gen", "1",
                         "--digest", "d", "--attempts", "1")
        self.assertEqual([x["status"] for x in out], ["cached", "cached"])

        # revocation lands while the predecessor is still missing
        r = self.open()
        res = r.revoke("s", 1)
        self.assertEqual(res["invalidated"], [201, 202])
        self.assertEqual(r.session_state("s")["expected"], 200)
        r.close()

        # writer fills the hole with the surviving key generation
        out = run_writer(self.db, "--session", "s", "--conn-gen", "1",
                         "--wire", "200", "--key-gen", "2", "--digest",
                         "d:200")
        self.assertEqual(out[0]["status"], "delivered")
        self.assertEqual(out[0]["released"], [200])

        r = self.open()
        try:
            self.assertEqual([g["abs_seq"] for g in r.gaps("s")], [201, 202])
            self.assertEqual([d["abs_seq"] for d in r.delivered("s")], [200])
            self.assertEqual(r.session_state("s")["expected"], 203)
        finally:
            r.close()


class TestWrapHandoff(WriterTestBase):
    def test_65535_to_0(self):
        r = self.open()
        r.establish("s", "t1", 1, 65534)
        r.add_key("s", 1, 65534, 65540)
        r.close()

        out = run_writer(self.db, "--session", "s", "--conn-gen", "1",
                         "--wire", "65534,65535,0,1", "--key-gen", "1",
                         "--digest", "d")
        self.assertEqual([x["abs_seq"] for x in out],
                         [65534, 65535, 65536, 65537])
        self.assertTrue(all(x["status"] == "delivered" for x in out))

        r = self.open()
        try:
            delivered = r.delivered("s")
            self.assertEqual([d["abs_seq"] for d in delivered],
                             [65534, 65535, 65536, 65537])
            self.assertEqual(r.session_state("s")["expected"], 65538)
        finally:
            r.close()


class TestOldConnectionReuse(WriterTestBase):
    def test_stale_generation_rejected(self):
        r = self.open()
        r.establish("s", "t1", 1, 1000)
        r.add_key("s", 1, 1000, 1999)
        r.close()

        out = run_writer(self.db, "--session", "s", "--conn-gen", "1",
                         "--wire", "1000", "--key-gen", "1",
                         "--digest", "d1000")
        self.assertEqual(out[0]["status"], "delivered")

        # reconnect under a newer generation
        r = self.open()
        r.establish("s", "t1", 2, 5000)
        r.add_key("s", 1, 5000, 5999)
        r.close()

        # writer still using the old connection number: rejected, no enqueue
        out = run_writer(self.db, "--session", "s", "--conn-gen", "1",
                         "--wire", "1001", "--key-gen", "1",
                         "--digest", "d1001")
        self.assertEqual(out[0]["code"], ERR["ERR_CONN_GEN"])

        r = self.open()
        try:
            self.assertEqual(r.receipts("s"), [])  # nothing under gen 2 yet
            self.assertEqual([d["abs_seq"] for d in
                              r.delivered("s", conn_gen=1)], [1000])
        finally:
            r.close()


class TestInvalidAuthNoEnqueue(WriterTestBase):
    def test_auth_failure_persists_nothing(self):
        r = self.open()
        r.establish("s", "t1", 1, 300)
        r.add_key("s", 1, 300, 399)
        r.close()

        out = run_writer(self.db, "--session", "s", "--conn-gen", "1",
                         "--wire", "300,301", "--key-gen", "1",
                         "--digest", "d", "--auth", "0", "--attempts", "3")
        self.assertEqual(len(out), 6)
        for line in out:
            self.assertEqual(line["code"], ERR["ERR_AUTH"])

        r = self.open()
        try:
            self.assertEqual(r.receipts("s"), [])
            self.assertEqual(r.session_state("s")["expected"], 300)
            self.assertEqual(r.session_state("s")["version"], 0)
        finally:
            r.close()


if __name__ == "__main__":
    unittest.main()
