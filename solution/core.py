"""Core receiver logic.

Rules implemented here (see README.md for the full specification):

* A session fixes (tenant, conn_gen, base_seq) at establishment. ``base_seq``
  is the first known absolute sequence number; ``expected`` starts there.
* Wire sequence numbers are 16-bit cyclic. The unique absolute sequence
  number is recovered only inside a 128-item window on both sides of
  ``expected``: candidates in ``[expected-128, expected+128)``.
  A wire sequence exactly half a cycle (32768) away from ``expected`` is
  ambiguous and must not be inferred.
* The left half of the window is used only to look up historical receipts
  (duplicate / conflict / gap answers). Anything at or beyond the right
  boundary is rejected.
* Old and new key generations may be valid simultaneously, but each key
  generation carries an absolute-sequence validity interval. Each absolute
  sequence number is received at most once: the first receipt binds the
  digest and the key evidence.
* Out-of-order messages are persistently cached and released to the queue
  only contiguously, starting at ``expected``.
* Revoking a key generation invalidates only that generation's not-yet-
  delivered data; an explicit gap is written for each such sequence.
  Already delivered history is never revoked.
* Every legal advancement of ``expected`` is guarded by the in-transaction
  row version (optimistic concurrency).
"""

from __future__ import annotations

import sqlite3

MOD = 65536          # 16-bit cyclic sequence space
HALF = MOD // 2      # 32768: exactly half a cycle -> ambiguous
WINDOW = 128         # recovery window on each side of `expected`

# ---------------------------------------------------------------------------
# Error codes (stable, documented in README.md)
# ---------------------------------------------------------------------------
ERR = {
    "ERR_AUTH": 1001,            # simulated authentication failed
    "ERR_SESSION_UNKNOWN": 1002,  # no such session
    "ERR_CONN_GEN": 1003,        # stale / reused connection generation
    "ERR_TENANT": 1004,          # tenant does not match the session
    "ERR_SEQ_RANGE": 1005,       # wire sequence not in 0..65535
    "ERR_HALF_CYCLE": 1006,      # exactly half a cycle away: ambiguous
    "ERR_WINDOW_LEFT": 1007,     # older than the left window boundary
    "ERR_WINDOW_RIGHT": 1008,    # at / beyond the right window boundary
    "ERR_KEY_UNKNOWN": 1009,     # key generation never registered
    "ERR_KEY_REVOKED": 1010,     # key generation was revoked
    "ERR_KEY_INTERVAL": 1011,    # key not valid at this absolute sequence
    "ERR_CONFLICT": 1012,        # same abs seq, different digest/key evidence
    "ERR_GAP": 1013,             # abs seq was invalidated by a revocation
    "ERR_SESSION_EXISTS": 1014,  # establish with same gen but different params
    "ERR_KEY_EXISTS": 1015,      # key generation registered twice
    "ERR_NO_RECEIPT": 1016,      # inside left window but no receipt on record
}
_ERR_NAME = {v: k for k, v in ERR.items()}

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT NOT NULL,
    conn_gen   INTEGER NOT NULL,
    tenant     TEXT NOT NULL,
    base_seq   INTEGER NOT NULL,
    expected   INTEGER NOT NULL,
    version    INTEGER NOT NULL,
    PRIMARY KEY (session_id, conn_gen)
);
CREATE TABLE IF NOT EXISTS current_gen (
    session_id TEXT PRIMARY KEY,
    conn_gen   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS keys (
    session_id TEXT NOT NULL,
    conn_gen   INTEGER NOT NULL,
    key_gen    INTEGER NOT NULL,
    lo         INTEGER NOT NULL,
    hi         INTEGER,               -- NULL means open ended
    revoked    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (session_id, conn_gen, key_gen)
);
-- One row per absolute sequence ever received (or invalidated).
-- state: 'cached' (persisted, not yet released),
--        'delivered' (released to the queue),
--        'gap' (invalidated by revocation before delivery).
-- The first receipt binds digest + key evidence permanently.
CREATE TABLE IF NOT EXISTS receipts (
    session_id TEXT NOT NULL,
    conn_gen   INTEGER NOT NULL,
    abs_seq    INTEGER NOT NULL,
    digest     TEXT NOT NULL,
    key_gen    INTEGER NOT NULL,
    state      TEXT NOT NULL,
    PRIMARY KEY (session_id, conn_gen, abs_seq)
);
"""


class ConcurrencyError(Exception):
    """Raised when the in-transaction version check fails."""


def recover_abs_seq(wire_seq, expected):
    """Recover the unique absolute sequence for a 16-bit wire sequence.

    Returns ``(abs_seq, None)`` or ``(None, error_code)``.
    Only a 128-item window on both sides of ``expected`` is considered.
    A wire sequence exactly half a cycle away is never inferred.
    """
    if not (0 <= wire_seq < MOD):
        return None, ERR["ERR_SEQ_RANGE"]
    # Centered distance in [-32768, 32767].
    delta = ((wire_seq - expected) % MOD + HALF) % MOD - HALF
    if delta == -HALF:
        # Exactly half a cycle away: ambiguous, must not infer.
        return None, ERR["ERR_HALF_CYCLE"]
    abs_seq = expected + delta
    if abs_seq < expected - WINDOW:
        return None, ERR["ERR_WINDOW_LEFT"]
    if abs_seq >= expected + WINDOW:
        return None, ERR["ERR_WINDOW_RIGHT"]
    return abs_seq, None


def _rej(code, **extra):
    out = {"status": "rejected", "code": code, "error": _ERR_NAME[code]}
    out.update(extra)
    return out


class Receiver:
    """SQLite-backed receiver. One instance == one DB connection."""

    def __init__(self, db_path, max_retries=8):
        self.db_path = str(db_path)
        self.max_retries = max_retries
        self.conn = sqlite3.connect(self.db_path, timeout=30.0)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self):
        self.conn.close()

    # ------------------------------------------------------------------
    # session / key management
    # ------------------------------------------------------------------
    def establish(self, session_id, tenant, conn_gen, base_seq):
        """Fix (tenant, conn_gen, base_seq) for a session.

        Re-establishing with a newer conn_gen is a reconnect: fresh state,
        old receipts stay archived under the old generation. Reusing an old
        (or the current) connection number is rejected.
        """
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                "SELECT conn_gen FROM current_gen WHERE session_id=?",
                (session_id,)).fetchone()
            if row is not None:
                if conn_gen <= row["conn_gen"]:
                    old = self.conn.execute(
                        "SELECT tenant, base_seq FROM sessions "
                        "WHERE session_id=? AND conn_gen=?",
                        (session_id, conn_gen)).fetchone()
                    if (conn_gen == row["conn_gen"] and old is not None
                            and old["tenant"] == tenant
                            and old["base_seq"] == base_seq):
                        self.conn.commit()
                        return {"status": "ok", "session": session_id,
                                "conn_gen": conn_gen, "idempotent": True}
                    self.conn.rollback()
                    return _rej(ERR["ERR_CONN_GEN"], session=session_id)
            self.conn.execute(
                "INSERT INTO sessions VALUES (?,?,?,?,?,0)",
                (session_id, conn_gen, tenant, base_seq, base_seq))
            self.conn.execute(
                "INSERT INTO current_gen VALUES (?,?) "
                "ON CONFLICT(session_id) DO UPDATE SET conn_gen=excluded.conn_gen",
                (session_id, conn_gen))
            self.conn.commit()
            return {"status": "ok", "session": session_id,
                    "conn_gen": conn_gen, "base_seq": base_seq}
        except Exception:
            self.conn.rollback()
            raise

    def add_key(self, session_id, key_gen, lo, hi=None):
        """Register a key generation valid for absolute seqs in [lo, hi].

        Intervals of different generations may overlap: old and new keys can
        be valid at the same time.
        """
        if hi is not None and hi < lo:
            raise ValueError("hi < lo")
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            gen = self._current_gen(session_id)
            if gen is None:
                self.conn.rollback()
                return _rej(ERR["ERR_SESSION_UNKNOWN"], session=session_id)
            exists = self.conn.execute(
                "SELECT 1 FROM keys WHERE session_id=? AND conn_gen=? AND key_gen=?",
                (session_id, gen, key_gen)).fetchone()
            if exists:
                self.conn.rollback()
                return _rej(ERR["ERR_KEY_EXISTS"], session=session_id,
                            key_gen=key_gen)
            self.conn.execute(
                "INSERT INTO keys VALUES (?,?,?,?,?,0)",
                (session_id, gen, key_gen, lo, hi))
            self.conn.commit()
            return {"status": "ok", "session": session_id, "key_gen": key_gen,
                    "lo": lo, "hi": hi}
        except Exception:
            self.conn.rollback()
            raise

    # ------------------------------------------------------------------
    # message intake
    # ------------------------------------------------------------------
    def recv(self, session_id, wire_seq, conn_gen, key_gen, digest,
             auth=True, tenant=None, before_commit=None):
        """Process one synthetic message.

        ``before_commit`` (if given) is called with the connection after all
        writes of this operation are done but before COMMIT; the crash-test
        worker uses it to kill itself at exactly that point.
        """
        if not auth:
            # Simulated authentication failure: nothing is persisted.
            return _rej(ERR["ERR_AUTH"], session=session_id)
        for attempt in range(self.max_retries):
            try:
                return self._recv_once(session_id, wire_seq, conn_gen,
                                       key_gen, digest, tenant, before_commit)
            except ConcurrencyError:
                continue
        raise ConcurrencyError("recv: too many version conflicts")

    def _recv_once(self, session_id, wire_seq, conn_gen, key_gen, digest,
                   tenant, before_commit):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            sess = self._load_session(session_id)
            if sess is None:
                self.conn.rollback()
                return _rej(ERR["ERR_SESSION_UNKNOWN"], session=session_id)
            if conn_gen != sess["conn_gen"]:
                self.conn.rollback()
                return _rej(ERR["ERR_CONN_GEN"], session=session_id,
                            expected_gen=sess["conn_gen"])
            if tenant is not None and tenant != sess["tenant"]:
                self.conn.rollback()
                return _rej(ERR["ERR_TENANT"], session=session_id)

            expected = sess["expected"]
            abs_seq, err = recover_abs_seq(wire_seq, expected)
            if err is not None:
                self.conn.rollback()
                return _rej(err, session=session_id)

            receipt = self.conn.execute(
                "SELECT digest, key_gen, state FROM receipts "
                "WHERE session_id=? AND conn_gen=? AND abs_seq=?",
                (session_id, conn_gen, abs_seq)).fetchone()
            if receipt is not None:
                # The first receipt already bound digest + key evidence.
                if receipt["state"] == "gap":
                    self.conn.rollback()
                    return _rej(ERR["ERR_GAP"], session=session_id,
                                abs_seq=abs_seq)
                if receipt["digest"] == digest and receipt["key_gen"] == key_gen:
                    self.conn.rollback()
                    return {"status": "duplicate", "session": session_id,
                            "abs_seq": abs_seq, "digest": receipt["digest"],
                            "key_gen": receipt["key_gen"],
                            "receipt_state": receipt["state"]}
                self.conn.rollback()
                return _rej(ERR["ERR_CONFLICT"], session=session_id,
                            abs_seq=abs_seq)

            if abs_seq < expected:
                # Inside the left window but no receipt on record.
                self.conn.rollback()
                return _rej(ERR["ERR_NO_RECEIPT"], session=session_id,
                            abs_seq=abs_seq)

            key = self.conn.execute(
                "SELECT lo, hi, revoked FROM keys "
                "WHERE session_id=? AND conn_gen=? AND key_gen=?",
                (session_id, conn_gen, key_gen)).fetchone()
            if key is None:
                self.conn.rollback()
                return _rej(ERR["ERR_KEY_UNKNOWN"], session=session_id,
                            abs_seq=abs_seq, key_gen=key_gen)
            if key["revoked"]:
                self.conn.rollback()
                return _rej(ERR["ERR_KEY_REVOKED"], session=session_id,
                            abs_seq=abs_seq, key_gen=key_gen)
            if abs_seq < key["lo"] or (key["hi"] is not None and abs_seq > key["hi"]):
                self.conn.rollback()
                return _rej(ERR["ERR_KEY_INTERVAL"], session=session_id,
                            abs_seq=abs_seq, key_gen=key_gen)

            # First receipt for this absolute sequence: bind evidence.
            self.conn.execute(
                "INSERT INTO receipts VALUES (?,?,?,?,?, 'cached')",
                (session_id, conn_gen, abs_seq, digest, key_gen))
            released = self._release_locked(sess)
            self._bump_version_locked(sess)
            if before_commit is not None:
                before_commit(self.conn)
            self.conn.commit()
            status = "delivered" if abs_seq in released else "cached"
            return {"status": status, "session": session_id,
                    "abs_seq": abs_seq, "released": released}
        except ConcurrencyError:
            self.conn.rollback()
            raise
        except Exception:
            self.conn.rollback()
            raise

    # ------------------------------------------------------------------
    # revocation
    # ------------------------------------------------------------------
    def revoke(self, session_id, key_gen):
        """Revoke a key generation.

        Only this generation's not-yet-delivered (cached) data is
        invalidated; an explicit gap row is written for each such absolute
        sequence. Delivered history is untouched.
        """
        for attempt in range(self.max_retries):
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                sess = self._load_session(session_id)
                if sess is None:
                    self.conn.rollback()
                    return _rej(ERR["ERR_SESSION_UNKNOWN"], session=session_id)
                key = self.conn.execute(
                    "SELECT revoked FROM keys WHERE session_id=? AND conn_gen=? "
                    "AND key_gen=?",
                    (session_id, sess["conn_gen"], key_gen)).fetchone()
                if key is None:
                    self.conn.rollback()
                    return _rej(ERR["ERR_KEY_UNKNOWN"], session=session_id,
                                key_gen=key_gen)
                if key["revoked"]:
                    self.conn.rollback()
                    return {"status": "ok", "session": session_id,
                            "key_gen": key_gen, "invalidated": [],
                            "idempotent": True}
                self.conn.execute(
                    "UPDATE keys SET revoked=1 WHERE session_id=? AND conn_gen=? "
                    "AND key_gen=?",
                    (session_id, sess["conn_gen"], key_gen))
                rows = self.conn.execute(
                    "SELECT abs_seq FROM receipts WHERE session_id=? AND conn_gen=? "
                    "AND key_gen=? AND state='cached' ORDER BY abs_seq",
                    (session_id, sess["conn_gen"], key_gen)).fetchall()
                invalidated = [r["abs_seq"] for r in rows]
                # Explicit gap: the first-receipt evidence is kept for audit,
                # the state becomes 'gap' and the data is dead.
                self.conn.execute(
                    "UPDATE receipts SET state='gap' WHERE session_id=? "
                    "AND conn_gen=? AND key_gen=? AND state='cached'",
                    (session_id, sess["conn_gen"], key_gen))
                released = self._release_locked(sess)
                self._bump_version_locked(sess)
                self.conn.commit()
                return {"status": "ok", "session": session_id,
                        "key_gen": key_gen, "invalidated": invalidated,
                        "released": released}
            except ConcurrencyError:
                self.conn.rollback()
                if attempt == self.max_retries - 1:
                    raise
            except Exception:
                self.conn.rollback()
                raise
        raise ConcurrencyError("revoke: too many version conflicts")

    # ------------------------------------------------------------------
    # queries
    # ------------------------------------------------------------------
    def delivered(self, session_id, conn_gen=None):
        gen = conn_gen if conn_gen is not None else self._current_gen(session_id)
        if gen is None:
            return []
        rows = self.conn.execute(
            "SELECT abs_seq, digest, key_gen FROM receipts WHERE session_id=? "
            "AND conn_gen=? AND state='delivered' ORDER BY abs_seq",
            (session_id, gen)).fetchall()
        return [dict(r) for r in rows]

    def gaps(self, session_id, conn_gen=None):
        gen = conn_gen if conn_gen is not None else self._current_gen(session_id)
        if gen is None:
            return []
        rows = self.conn.execute(
            "SELECT abs_seq, digest, key_gen FROM receipts WHERE session_id=? "
            "AND conn_gen=? AND state='gap' ORDER BY abs_seq",
            (session_id, gen)).fetchall()
        return [dict(r) for r in rows]

    def cached(self, session_id, conn_gen=None):
        gen = conn_gen if conn_gen is not None else self._current_gen(session_id)
        if gen is None:
            return []
        rows = self.conn.execute(
            "SELECT abs_seq, digest, key_gen FROM receipts WHERE session_id=? "
            "AND conn_gen=? AND state='cached' ORDER BY abs_seq",
            (session_id, gen)).fetchall()
        return [dict(r) for r in rows]

    def receipts(self, session_id, conn_gen=None):
        gen = conn_gen if conn_gen is not None else self._current_gen(session_id)
        if gen is None:
            return []
        rows = self.conn.execute(
            "SELECT abs_seq, digest, key_gen, state FROM receipts "
            "WHERE session_id=? AND conn_gen=? ORDER BY abs_seq",
            (session_id, gen)).fetchall()
        return [dict(r) for r in rows]

    def session_state(self, session_id):
        sess = self._load_session(session_id)
        if sess is None:
            return None
        return {"session_id": sess["session_id"], "tenant": sess["tenant"],
                "conn_gen": sess["conn_gen"], "base_seq": sess["base_seq"],
                "expected": sess["expected"], "version": sess["version"]}

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------
    def _current_gen(self, session_id):
        row = self.conn.execute(
            "SELECT conn_gen FROM current_gen WHERE session_id=?",
            (session_id,)).fetchone()
        return row["conn_gen"] if row else None

    def _load_session(self, session_id):
        gen = self._current_gen(session_id)
        if gen is None:
            return None
        row = self.conn.execute(
            "SELECT * FROM sessions WHERE session_id=? AND conn_gen=?",
            (session_id, gen)).fetchone()
        return dict(row) if row is not None else None

    def _release_locked(self, sess):
        """Contiguously release cached rows / skip gaps from `expected`.

        Must be called inside a BEGIN IMMEDIATE transaction. Returns the
        list of absolute sequences delivered by this call.
        """
        released = []
        expected = sess["expected"]
        while True:
            row = self.conn.execute(
                "SELECT state FROM receipts WHERE session_id=? AND conn_gen=? "
                "AND abs_seq=?",
                (sess["session_id"], sess["conn_gen"], expected)).fetchone()
            if row is None:
                break
            if row["state"] == "cached":
                self.conn.execute(
                    "UPDATE receipts SET state='delivered' WHERE session_id=? "
                    "AND conn_gen=? AND abs_seq=?",
                    (sess["session_id"], sess["conn_gen"], expected))
                released.append(expected)
            elif row["state"] == "gap":
                pass  # explicit gap: skip, data already invalidated
            else:  # already delivered (should not happen at `expected`)
                break
            expected += 1
        if expected != sess["expected"]:
            self.conn.execute(
                "UPDATE sessions SET expected=? WHERE session_id=? AND conn_gen=?",
                (expected, sess["session_id"], sess["conn_gen"]))
            sess["expected"] = expected
        return released

    def _bump_version_locked(self, sess):
        """Advance the session guarded by the in-transaction version."""
        cur = self.conn.execute(
            "UPDATE sessions SET version=version+1 "
            "WHERE session_id=? AND conn_gen=? AND version=?",
            (sess["session_id"], sess["conn_gen"], sess["version"]))
        if cur.rowcount != 1:
            raise ConcurrencyError(
                f"version conflict on session {sess['session_id']!r}")
        sess["version"] += 1
