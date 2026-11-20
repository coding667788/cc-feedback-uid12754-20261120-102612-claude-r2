"""Unit tests for the core receiver rules."""

import unittest

from solution.core import Receiver, ConcurrencyError, ERR, recover_abs_seq


def make(base=1000, keys=((1, 1000, 1999),), tenant="t1", sid="s"):
    r = Receiver(":memory:")
    assert r.establish(sid, tenant, 1, base)["status"] == "ok"
    for kg, lo, hi in keys:
        assert r.add_key(sid, kg, lo, hi)["status"] == "ok"
    return r


class TestRecover(unittest.TestCase):
    def test_window_edges(self):
        # expected = 5000; window is [4872, 5128)
        self.assertEqual(recover_abs_seq(5000, 5000), (5000, None))
        self.assertEqual(recover_abs_seq(5127, 5000), (5127, None))
        self.assertEqual(recover_abs_seq(4872, 5000), (4872, None))
        self.assertEqual(recover_abs_seq(5128, 5000),
                         (None, ERR["ERR_WINDOW_RIGHT"]))
        self.assertEqual(recover_abs_seq(4871, 5000),
                         (None, ERR["ERR_WINDOW_LEFT"]))

    def test_half_cycle_not_inferred(self):
        self.assertEqual(recover_abs_seq((5000 + 32768) % 65536, 5000),
                         (None, ERR["ERR_HALF_CYCLE"]))
        # one tick inside half cycle is still just a window violation
        self.assertEqual(recover_abs_seq((5000 + 32767) % 65536, 5000),
                         (None, ERR["ERR_WINDOW_RIGHT"]))
        self.assertEqual(recover_abs_seq((5000 - 32767) % 65536, 5000),
                         (None, ERR["ERR_WINDOW_LEFT"]))

    def test_wrap_recovery(self):
        self.assertEqual(recover_abs_seq(0, 65535), (65536, None))
        # wire 65535 sits inside the left half of the window here
        self.assertEqual(recover_abs_seq(65535, 65536 + 100), (65535, None))
        self.assertEqual(recover_abs_seq(0, 65536 + 100), (65536, None))
        self.assertEqual(recover_abs_seq(1, 65534), (65537, None))

    def test_wire_range(self):
        self.assertEqual(recover_abs_seq(65536, 0)[1], ERR["ERR_SEQ_RANGE"])
        self.assertEqual(recover_abs_seq(-1, 0)[1], ERR["ERR_SEQ_RANGE"])


class TestDelivery(unittest.TestCase):
    def test_in_order(self):
        r = make()
        for i in range(3):
            res = r.recv("s", 1000 + i, 1, 1, f"d{1000+i}")
            self.assertEqual(res["status"], "delivered")
            self.assertEqual(res["released"], [1000 + i])
        self.assertEqual([d["abs_seq"] for d in r.delivered("s")],
                         [1000, 1001, 1002])
        self.assertEqual(r.session_state("s")["expected"], 1003)

    def test_out_of_order_contiguous_release(self):
        r = make(base=100, keys=((1, 100, 999),))
        self.assertEqual(r.recv("s", 102, 1, 1, "d102")["status"], "cached")
        self.assertEqual(r.recv("s", 104, 1, 1, "d104")["status"], "cached")
        self.assertEqual(r.recv("s", 101, 1, 1, "d101")["status"], "cached")
        res = r.recv("s", 100, 1, 1, "d100")
        self.assertEqual(res["status"], "delivered")
        self.assertEqual(res["released"], [100, 101, 102])
        res = r.recv("s", 103, 1, 1, "d103")
        self.assertEqual(res["released"], [103, 104])
        self.assertEqual([d["abs_seq"] for d in r.delivered("s")],
                         [100, 101, 102, 103, 104])

    def test_wrap_65535_to_0(self):
        r = make(base=65534, keys=((1, 65534, 65540),))
        wires = [65534, 65535, 0, 1]
        for i, w in enumerate(wires):
            res = r.recv("s", w, 1, 1, f"d{65534 + i}")
            self.assertEqual(res["status"], "delivered")
            self.assertEqual(res["abs_seq"], 65534 + i)
        self.assertEqual([d["abs_seq"] for d in r.delivered("s")],
                         [65534, 65535, 65536, 65537])
        self.assertEqual(r.session_state("s")["expected"], 65538)


class TestReceiptsAndKeys(unittest.TestCase):
    def test_first_receipt_binds_evidence(self):
        r = make(base=100, keys=((1, 100, 199), (2, 150, 299)))
        r.recv("s", 100, 1, 1, "d100")
        r.recv("s", 160, 1, 2, "d160")
        # same abs seq, different key + digest -> conflict
        res = r.recv("s", 160, 1, 1, "d160b")
        self.assertEqual(res["code"], ERR["ERR_CONFLICT"])
        self.assertEqual(res["abs_seq"], 160)
        # exact retransmission -> duplicate answered from the receipt
        res = r.recv("s", 160, 1, 2, "d160")
        self.assertEqual(res["status"], "duplicate")
        self.assertEqual(res["receipt_state"], "cached")
        self.assertEqual(res["key_gen"], 2)
        # only one receipt row exists for abs 160
        rows = [x for x in r.receipts("s") if x["abs_seq"] == 160]
        self.assertEqual(len(rows), 1)

    def test_key_interval_enforced(self):
        r = make(base=100, keys=((1, 100, 199), (2, 150, 299)))
        r.recv("s", 100, 1, 1, "d100")
        res = r.recv("s", 140, 1, 2, "d140")  # 140 < key2.lo
        self.assertEqual(res["code"], ERR["ERR_KEY_INTERVAL"])
        res = r.recv("s", 150, 1, 3, "d150")  # unknown key generation
        self.assertEqual(res["code"], ERR["ERR_KEY_UNKNOWN"])
        # old and new keys are both valid inside the overlap
        self.assertEqual(r.recv("s", 150, 1, 1, "d150")["status"], "cached")
        self.assertEqual(r.recv("s", 151, 1, 2, "d151")["status"], "cached")

    def test_invalid_auth_persists_nothing(self):
        r = make(base=300, keys=((1, 300, 399),))
        res = r.recv("s", 300, 1, 1, "d300", auth=False)
        self.assertEqual(res["code"], ERR["ERR_AUTH"])
        self.assertEqual(r.receipts("s"), [])
        self.assertEqual(r.session_state("s")["expected"], 300)
        self.assertEqual(r.session_state("s")["version"], 0)

    def test_tenant_mismatch(self):
        r = make(base=300, keys=((1, 300, 399),))
        res = r.recv("s", 300, 1, 1, "d300", tenant="other")
        self.assertEqual(res["code"], ERR["ERR_TENANT"])

    def test_window_rejections(self):
        r = make(base=5000, keys=((1, 5000, 9999),))
        self.assertEqual(r.recv("s", 5127, 1, 1, "d5127")["status"], "cached")
        res = r.recv("s", 5128, 1, 1, "d5128")
        self.assertEqual(res["code"], ERR["ERR_WINDOW_RIGHT"])
        res = r.recv("s", 4871, 1, 1, "d4871")
        self.assertEqual(res["code"], ERR["ERR_WINDOW_LEFT"])
        res = r.recv("s", 37768, 1, 1, "dHalf")
        self.assertEqual(res["code"], ERR["ERR_HALF_CYCLE"])
        # inside the left window but never received -> no receipt on record
        res = r.recv("s", 4999, 1, 1, "d4999")
        self.assertEqual(res["code"], ERR["ERR_NO_RECEIPT"])


class TestRevocation(unittest.TestCase):
    def test_revoke_invalidates_only_undelivered(self):
        r = make(base=200, keys=((1, 200, 299), (2, 200, 299)))
        r.recv("s", 200, 1, 1, "d200")           # delivered with key 1
        r.recv("s", 202, 1, 1, "d202")           # cached (201 missing)
        r.recv("s", 203, 1, 1, "d203")           # cached
        r.recv("s", 205, 1, 2, "d205")           # cached, other generation
        res = r.revoke("s", 1)
        self.assertEqual(res["status"], "ok")
        self.assertEqual(res["invalidated"], [202, 203])
        # explicit gaps written, delivered history untouched
        self.assertEqual([g["abs_seq"] for g in r.gaps("s")], [202, 203])
        self.assertEqual([d["abs_seq"] for d in r.delivered("s")], [200])
        # 201 still missing: expected cannot advance over it
        self.assertEqual(r.session_state("s")["expected"], 201)
        # gap answers are explicit
        res = r.recv("s", 202, 1, 2, "d202")
        self.assertEqual(res["code"], ERR["ERR_GAP"])
        # revoked key can no longer be used
        res = r.recv("s", 206, 1, 1, "d206")
        self.assertEqual(res["code"], ERR["ERR_KEY_REVOKED"])
        # filling the hole releases over the gaps
        res = r.recv("s", 201, 1, 2, "d201")
        self.assertEqual(res["released"], [201])
        self.assertEqual(r.session_state("s")["expected"], 204)
        res = r.recv("s", 204, 1, 2, "d204")
        self.assertEqual(res["released"], [204, 205])
        self.assertEqual([d["abs_seq"] for d in r.delivered("s")],
                         [200, 201, 204, 205])
        # revocation is idempotent
        res = r.revoke("s", 1)
        self.assertTrue(res.get("idempotent"))

    def test_revoke_with_missing_predecessor(self):
        # predecessor of the revoked data is itself missing
        r = make(base=200, keys=((1, 200, 299), (2, 200, 299)))
        r.recv("s", 201, 1, 1, "d201")
        r.recv("s", 202, 1, 1, "d202")
        res = r.revoke("s", 1)
        self.assertEqual(res["invalidated"], [201, 202])
        # 200 still missing: nothing can advance yet
        self.assertEqual(r.session_state("s")["expected"], 200)
        res = r.recv("s", 200, 1, 2, "d200")
        self.assertEqual(res["released"], [200])
        self.assertEqual(r.session_state("s")["expected"], 203)
        self.assertEqual([g["abs_seq"] for g in r.gaps("s")], [201, 202])


class TestConnectionGeneration(unittest.TestCase):
    def test_old_connection_number_reuse_rejected(self):
        r = make(base=1000, keys=((1, 1000, 1999),))
        r.recv("s", 1000, 1, 1, "d1000")
        # reconnect with a newer generation
        res = r.establish("s", "t1", 2, 5000)
        self.assertEqual(res["status"], "ok")
        r.add_key("s", 1, 5000, 5999)
        # messages on the old connection number are rejected
        res = r.recv("s", 1001, 1, 1, "d1001")
        self.assertEqual(res["code"], ERR["ERR_CONN_GEN"])
        # re-establishing the old connection number is rejected
        res = r.establish("s", "t1", 1, 1000)
        self.assertEqual(res["code"], ERR["ERR_CONN_GEN"])
        # new generation works and starts from its own base
        res = r.recv("s", 5000, 2, 1, "d5000")
        self.assertEqual(res["status"], "delivered")
        self.assertEqual([d["abs_seq"] for d in r.delivered("s")], [5000])
        # old generation history is archived, not lost
        self.assertEqual([d["abs_seq"] for d in r.delivered("s", conn_gen=1)],
                         [1000])


class TestVersionedAdvancement(unittest.TestCase):
    def test_version_counts_committed_mutations(self):
        r = make(base=100, keys=((1, 100, 999), (2, 100, 999)))
        expected_version = 0
        for i, w in enumerate([102, 101, 100, 199]):  # all four accepted
            res = r.recv("s", w, 1, 1, f"d{w}")
            if res["status"] in ("cached", "delivered"):
                expected_version += 1
        r.revoke("s", 1)
        expected_version += 1
        # rejections do not bump the version
        r.recv("s", 100, 1, 1, "d100")          # duplicate
        r.recv("s", 150, 1, 9, "dx")            # unknown key
        self.assertEqual(r.session_state("s")["version"], expected_version)

    def test_stale_version_is_rejected(self):
        r = make(base=100, keys=((1, 100, 999),))
        sess = r.session_state("s")
        r.conn.execute("BEGIN IMMEDIATE")
        try:
            with self.assertRaises(ConcurrencyError):
                r._bump_version_locked(
                    {"session_id": "s", "conn_gen": 1,
                     "version": sess["version"] + 5})  # stale
        finally:
            r.conn.rollback()


if __name__ == "__main__":
    unittest.main()
