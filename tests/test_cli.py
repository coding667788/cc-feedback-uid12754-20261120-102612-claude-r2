"""End-to-end test of the CLI against cases.json with concrete assertions."""

import json
import os
import subprocess
import sys
import tempfile
import unittest

from solution.core import ERR

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CASES = os.path.join(ROOT, "cases.json")


class TestCli(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="cli_test_")
        cls.out = os.path.join(cls.tmp, "result.json")
        proc = subprocess.run(
            [sys.executable, "-m", "solution", "--input", CASES,
             "--output", cls.out],
            capture_output=True, text=True, timeout=300, cwd=ROOT)
        assert proc.returncode == 0, proc.stderr
        with open(cls.out, "r", encoding="utf-8") as fh:
            cls.result = json.load(fh)
        cls.cases = {c["name"]: c for c in cls.result["cases"]}

    def ops(self, name):
        return self.cases[name]["ops"]

    def statuses(self, name):
        return [o.get("status") for o in self.ops(name)]

    def delivered(self, name, sid):
        return [d["abs_seq"]
                for d in self.cases[name]["sessions"][sid]["delivered"]]

    def gaps(self, name, sid):
        return [g["abs_seq"]
                for g in self.cases[name]["sessions"][sid]["gaps"]]

    def test_all_cases_present(self):
        self.assertEqual(len(self.result["cases"]), 9)

    def test_in_order(self):
        self.assertEqual(self.statuses("in_order_delivery"),
                         ["ok", "ok", "delivered", "delivered", "delivered"])
        self.assertEqual(self.delivered("in_order_delivery", "s1"),
                         [1000, 1001, 1002])
        state = self.cases["in_order_delivery"]["sessions"]["s1"]["state"]
        self.assertEqual(state["expected"], 1003)
        self.assertEqual(state["version"], 3)

    def test_out_of_order(self):
        ops = self.ops("out_of_order_contiguous_release")
        self.assertEqual([o["status"] for o in ops[2:]],
                         ["cached", "cached", "cached",
                          "delivered", "delivered"])
        self.assertEqual(ops[5]["released"], [100, 101, 102])
        self.assertEqual(ops[6]["released"], [103, 104])
        self.assertEqual(
            self.delivered("out_of_order_contiguous_release", "s2"),
            [100, 101, 102, 103, 104])

    def test_wrap(self):
        ops = self.ops("wrap_65535_to_0")
        self.assertEqual([o["abs_seq"] for o in ops[2:]],
                         [65534, 65535, 65536, 65537])
        self.assertEqual(self.delivered("wrap_65535_to_0", "s3"),
                         [65534, 65535, 65536, 65537])

    def test_window_boundaries(self):
        ops = self.ops("window_boundaries")
        self.assertEqual(ops[2]["status"], "cached")       # expected+127
        self.assertEqual(ops[2]["abs_seq"], 5127)
        self.assertEqual(ops[3]["code"], ERR["ERR_WINDOW_RIGHT"])
        self.assertEqual(ops[4]["code"], ERR["ERR_NO_RECEIPT"])
        self.assertEqual(ops[5]["code"], ERR["ERR_WINDOW_LEFT"])
        self.assertEqual(ops[6]["code"], ERR["ERR_HALF_CYCLE"])

    def test_dual_key(self):
        ops = self.ops("dual_key_overlap_and_conflict")
        self.assertEqual(ops[3]["status"], "delivered")    # 100 via key 1
        self.assertEqual(ops[4]["status"], "cached")       # 160 via key 2
        self.assertEqual(ops[5]["code"], ERR["ERR_CONFLICT"])
        self.assertEqual(ops[5]["abs_seq"], 160)
        self.assertEqual(ops[6]["status"], "duplicate")
        self.assertEqual(ops[6]["receipt_state"], "cached")
        self.assertEqual(ops[7]["code"], ERR["ERR_KEY_INTERVAL"])
        self.assertEqual(ops[8]["code"], ERR["ERR_CONFLICT"])

    def test_revoke(self):
        ops = self.ops("revoke_with_missing_predecessor")
        self.assertEqual(ops[3]["status"], "delivered")    # 200 via key 1
        self.assertEqual(ops[4]["status"], "cached")       # 202 (201 missing)
        self.assertEqual(ops[7]["invalidated"], [202, 203])
        self.assertEqual(ops[8]["released"], [201])
        self.assertEqual(ops[9]["released"], [204, 205])
        self.assertEqual(ops[10]["code"], ERR["ERR_KEY_REVOKED"])
        self.assertEqual(ops[11]["code"], ERR["ERR_GAP"])
        self.assertEqual(ops[12]["status"], "duplicate")
        self.assertEqual(ops[12]["receipt_state"], "delivered")
        self.assertEqual(
            self.delivered("revoke_with_missing_predecessor", "s6"),
            [200, 201, 204, 205])
        self.assertEqual(
            self.gaps("revoke_with_missing_predecessor", "s6"), [202, 203])

    def test_conn_gen_reuse(self):
        ops = self.ops("connection_generation_reuse")
        self.assertEqual(ops[2]["status"], "delivered")
        self.assertEqual(ops[3]["status"], "ok")           # reconnect gen 2
        self.assertEqual(ops[5]["code"], ERR["ERR_CONN_GEN"])
        self.assertEqual(ops[6]["code"], ERR["ERR_CONN_GEN"])
        self.assertEqual(ops[7]["status"], "delivered")
        self.assertEqual(
            self.delivered("connection_generation_reuse", "s7"), [5000])

    def test_invalid_auth(self):
        ops = self.ops("invalid_auth_no_enqueue")
        self.assertEqual(ops[2]["code"], ERR["ERR_AUTH"])
        self.assertEqual(ops[3]["code"], ERR["ERR_AUTH"])
        self.assertEqual(ops[4]["status"], "delivered")
        self.assertEqual(self.delivered("invalid_auth_no_enqueue", "s8"),
                         [300])

    def test_receipt_binding(self):
        ops = self.ops("receipt_binding_and_duplicates")
        self.assertEqual(ops[2]["status"], "delivered")
        self.assertEqual(ops[3]["status"], "duplicate")
        self.assertEqual(ops[3]["receipt_state"], "delivered")
        self.assertEqual(ops[4]["status"], "cached")
        self.assertEqual(ops[5]["status"], "duplicate")
        self.assertEqual(ops[5]["receipt_state"], "cached")
        self.assertEqual(ops[6]["code"], ERR["ERR_CONFLICT"])
        self.assertEqual(self.delivered("receipt_binding_and_duplicates",
                                        "s9"), [700])


if __name__ == "__main__":
    unittest.main()
