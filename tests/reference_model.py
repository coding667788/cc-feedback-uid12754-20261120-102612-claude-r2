"""Independent fixed-window reference model.

Written separately from solution.core (pure in-memory dicts, no SQLite) so
the randomized replay tests can cross-check the real implementation against
an independent reading of the same rules:

* 16-bit cyclic wire sequence; unique absolute sequence recovered only
  inside [expected-128, expected+128); exactly half a cycle is ambiguous.
* Left half of the window: historical receipt lookup only.
* Key generations carry absolute-sequence intervals and may overlap.
* First receipt binds digest + key evidence; each abs seq received once.
* Revocation invalidates only undelivered data of that generation and
  leaves an explicit gap; delivered history is never revoked.
"""

MOD = 65536
HALF = 32768
WINDOW = 128

ERR = {
    "ERR_AUTH": 1001,
    "ERR_SESSION_UNKNOWN": 1002,
    "ERR_CONN_GEN": 1003,
    "ERR_TENANT": 1004,
    "ERR_SEQ_RANGE": 1005,
    "ERR_HALF_CYCLE": 1006,
    "ERR_WINDOW_LEFT": 1007,
    "ERR_WINDOW_RIGHT": 1008,
    "ERR_KEY_UNKNOWN": 1009,
    "ERR_KEY_REVOKED": 1010,
    "ERR_KEY_INTERVAL": 1011,
    "ERR_CONFLICT": 1012,
    "ERR_GAP": 1013,
    "ERR_SESSION_EXISTS": 1014,
    "ERR_KEY_EXISTS": 1015,
    "ERR_NO_RECEIPT": 1016,
}
_NAME = {v: k for k, v in ERR.items()}


def _rej(code, **extra):
    return {"status": "rejected", "code": code, "error": _NAME[code], **extra}


class RefReceiver:
    def __init__(self):
        self.sessions = {}  # sid -> state dict

    # ------------------------------------------------------------------
    def establish(self, sid, tenant, conn_gen, base_seq):
        cur = self.sessions.get(sid)
        if cur is not None:
            if conn_gen <= cur["conn_gen"]:
                if (conn_gen == cur["conn_gen"] and cur["tenant"] == tenant
                        and cur["base_seq"] == base_seq):
                    return {"status": "ok", "idempotent": True}
                return _rej(ERR["ERR_CONN_GEN"])
        self.sessions[sid] = {
            "tenant": tenant, "conn_gen": conn_gen, "base_seq": base_seq,
            "expected": base_seq, "keys": {}, "receipts": {},
        }
        return {"status": "ok"}

    def add_key(self, sid, key_gen, lo, hi=None):
        s = self.sessions.get(sid)
        if s is None:
            return _rej(ERR["ERR_SESSION_UNKNOWN"])
        if key_gen in s["keys"]:
            return _rej(ERR["ERR_KEY_EXISTS"])
        s["keys"][key_gen] = {"lo": lo, "hi": hi, "revoked": False}
        return {"status": "ok"}

    # ------------------------------------------------------------------
    def recv(self, sid, wire, conn_gen, key_gen, digest, auth=True,
             tenant=None):
        if not auth:
            return _rej(ERR["ERR_AUTH"])
        s = self.sessions.get(sid)
        if s is None:
            return _rej(ERR["ERR_SESSION_UNKNOWN"])
        if conn_gen != s["conn_gen"]:
            return _rej(ERR["ERR_CONN_GEN"])
        if tenant is not None and tenant != s["tenant"]:
            return _rej(ERR["ERR_TENANT"])
        if not (0 <= wire < MOD):
            return _rej(ERR["ERR_SEQ_RANGE"])
        delta = ((wire - s["expected"]) % MOD + HALF) % MOD - HALF
        if delta == -HALF:
            return _rej(ERR["ERR_HALF_CYCLE"])
        abs_seq = s["expected"] + delta
        if abs_seq < s["expected"] - WINDOW:
            return _rej(ERR["ERR_WINDOW_LEFT"])
        if abs_seq >= s["expected"] + WINDOW:
            return _rej(ERR["ERR_WINDOW_RIGHT"])
        r = s["receipts"].get(abs_seq)
        if r is not None:
            if r["state"] == "gap":
                return _rej(ERR["ERR_GAP"], abs_seq=abs_seq)
            if r["digest"] == digest and r["key_gen"] == key_gen:
                return {"status": "duplicate", "abs_seq": abs_seq,
                        "digest": r["digest"], "key_gen": r["key_gen"],
                        "receipt_state": r["state"]}
            return _rej(ERR["ERR_CONFLICT"], abs_seq=abs_seq)
        if abs_seq < s["expected"]:
            return _rej(ERR["ERR_NO_RECEIPT"], abs_seq=abs_seq)
        k = s["keys"].get(key_gen)
        if k is None:
            return _rej(ERR["ERR_KEY_UNKNOWN"], abs_seq=abs_seq)
        if k["revoked"]:
            return _rej(ERR["ERR_KEY_REVOKED"], abs_seq=abs_seq)
        if abs_seq < k["lo"] or (k["hi"] is not None and abs_seq > k["hi"]):
            return _rej(ERR["ERR_KEY_INTERVAL"], abs_seq=abs_seq)
        s["receipts"][abs_seq] = {"digest": digest, "key_gen": key_gen,
                                  "state": "cached"}
        released = self._release(s)
        return {"status": "delivered" if abs_seq in released else "cached",
                "abs_seq": abs_seq, "released": released}

    def revoke(self, sid, key_gen):
        s = self.sessions.get(sid)
        if s is None:
            return _rej(ERR["ERR_SESSION_UNKNOWN"])
        k = s["keys"].get(key_gen)
        if k is None:
            return _rej(ERR["ERR_KEY_UNKNOWN"])
        if k["revoked"]:
            return {"status": "ok", "invalidated": [], "idempotent": True}
        k["revoked"] = True
        invalidated = sorted(a for a, r in s["receipts"].items()
                             if r["key_gen"] == key_gen
                             and r["state"] == "cached")
        for a in invalidated:
            s["receipts"][a]["state"] = "gap"
        released = self._release(s)
        return {"status": "ok", "invalidated": invalidated,
                "released": released}

    # ------------------------------------------------------------------
    def _release(self, s):
        released = []
        while True:
            r = s["receipts"].get(s["expected"])
            if r is None:
                break
            if r["state"] == "cached":
                r["state"] = "delivered"
                released.append(s["expected"])
            elif r["state"] == "gap":
                pass
            else:
                break
            s["expected"] += 1
        return released

    def delivered(self, sid):
        s = self.sessions[sid]
        return [a for a in sorted(s["receipts"])
                if s["receipts"][a]["state"] == "delivered"]

    def gaps(self, sid):
        s = self.sessions[sid]
        return [a for a in sorted(s["receipts"])
                if s["receipts"][a]["state"] == "gap"]
