"""Cross-check the SQLite receiver against an independent fixed-window
reference model over randomized synthetic sequences and full window
boundaries."""

import random
import unittest

from solution.core import Receiver
from tests.reference_model import RefReceiver


def normalize(res):
    """Keep only the fields both implementations must agree on."""
    out = {"status": res["status"]}
    for key in ("code", "abs_seq", "receipt_state"):
        if key in res:
            out[key] = res[key]
    if "released" in res:
        out["released"] = list(res["released"])
    if "invalidated" in res:
        out["invalidated"] = list(res["invalidated"])
    return out


class TestBoundarySweep(unittest.TestCase):
    """Every offset around the window edges, the half-cycle point and the
    65535 -> 0 wrap, on a fresh session each time."""

    def test_offsets(self):
        offsets = list(range(-131, 132))
        offsets += [32766, 32767, 32768, -32766, -32767, -32768, 65535]
        for base in (1_000_000, 65530):
            for off in offsets:
                with self.subTest(base=base, off=off):
                    target = base + off
                    wire = target % 65536
                    # effective signed distance after 16-bit wrap-around
                    eff = ((off % 65536) + 32768) % 65536 - 32768
                    real = Receiver(":memory:")
                    real.establish("s", "t1", 1, base)
                    real.add_key("s", 1, 0, None)  # open interval
                    ref = RefReceiver()
                    ref.establish("s", "t1", 1, base)
                    ref.add_key("s", 1, 0, None)
                    got = normalize(real.recv("s", wire, 1, 1, f"d{target}"))
                    want = normalize(ref.recv("s", wire, 1, 1, f"d{target}"))
                    self.assertEqual(got, want)
                    # explicit concrete expectations for the edges
                    if eff == -32768:
                        self.assertEqual(got["code"], 1006)  # ERR_HALF_CYCLE
                    elif eff < -128:
                        self.assertEqual(got["code"], 1007)  # ERR_WINDOW_LEFT
                    elif -128 <= eff <= -1:
                        self.assertEqual(got["code"], 1016)  # ERR_NO_RECEIPT
                    elif 0 <= eff <= 127:
                        self.assertIn(got["status"], ("cached", "delivered"))
                        self.assertEqual(got["abs_seq"], target)
                    else:
                        self.assertEqual(got["code"], 1008)  # ERR_WINDOW_RIGHT


class TestRandomizedReplay(unittest.TestCase):
    def test_random_sequences_match_reference(self):
        rng = random.Random(20261120)
        for trial in range(6):
            with self.subTest(trial=trial):
                self._run_trial(rng)

    def _run_trial(self, rng):
        real = Receiver(":memory:")
        ref = RefReceiver()
        sid = "s"
        base = rng.choice([0, 100, 65500, 1_000_000])
        tenant = "t1"
        self.assertEqual(real.establish(sid, tenant, 1, base)["status"], "ok")
        ref.establish(sid, tenant, 1, base)
        # three key generations with overlapping absolute intervals
        for kg in (1, 2, 3):
            lo = base + rng.randint(0, 40)
            hi = lo + rng.randint(50, 300)
            self.assertEqual(real.add_key(sid, kg, lo, hi)["status"], "ok")
            ref.add_key(sid, kg, lo, hi)

        for step in range(1500):
            kind = rng.random()
            if kind < 0.04:
                kg = rng.randint(1, 3)
                got = normalize(real.revoke(sid, kg))
                want = normalize(ref.revoke(sid, kg))
            else:
                expected = real.session_state(sid)["expected"]
                roll = rng.random()
                if roll < 0.60:
                    target = expected + rng.randint(-140, 140)
                elif roll < 0.75:
                    target = expected + rng.choice([-128, -1, 0, 1, 127, 128])
                elif roll < 0.85:
                    target = expected + rng.choice([32767, 32768, -32768])
                else:
                    target = rng.randint(0, 2000000)
                wire = target % 65536
                kg = rng.randint(1, 4)  # 4 is never registered
                # mostly consistent digests, sometimes a conflicting one
                digest = f"d{target}" if rng.random() > 0.05 else f"x{target}"
                auth = rng.random() > 0.05
                got = normalize(real.recv(sid, wire, 1, kg, digest, auth=auth))
                want = normalize(ref.recv(sid, wire, 1, kg, digest, auth=auth))
            self.assertEqual(got, want, f"step {step}")

        self.assertEqual(
            [d["abs_seq"] for d in real.delivered(sid)], ref.delivered(sid))
        self.assertEqual(
            [g["abs_seq"] for g in real.gaps(sid)], ref.gaps(sid))
        self.assertEqual(real.session_state(sid)["expected"],
                         ref.sessions[sid]["expected"])
        # no absolute sequence was ever received twice
        rows = real.receipts(sid)
        self.assertEqual(len(rows), len({r["abs_seq"] for r in rows}))


if __name__ == "__main__":
    unittest.main()
