"""Crash-test worker process.

    python -m solution.worker --db DB --script SCRIPT.json --out OUT.jsonl
        [--crash OP_INDEX:PHASE]

PHASE is one of:
  * ``pre_commit``  - kill ourselves after the out-of-order message rows are
                      saved inside the transaction but before COMMIT
                      (the transaction then rolls back);
  * ``post_commit`` - kill ourselves after the queue advancement committed
                      but before the reply line is written.

The worker kills itself with ``os._exit(3)`` at the requested point, exactly
like an abrupt process termination. Restarting the worker with the same
script (and no --crash) replays every op; persistent receipts make the
replay idempotent.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from .core import Receiver
from .runner import run_op

CRASH_EXIT = 3


def main(argv=None):
    parser = argparse.ArgumentParser(prog="solution.worker")
    parser.add_argument("--db", required=True)
    parser.add_argument("--script", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--crash", default=None,
                        help="OP_INDEX:PHASE with PHASE in "
                             "{pre_commit, post_commit}")
    args = parser.parse_args(argv)

    crash_at = None
    if args.crash:
        idx, phase = args.crash.split(":")
        crash_at = (int(idx), phase)

    with open(args.script, "r", encoding="utf-8") as fh:
        ops = json.load(fh)["ops"]

    receiver = Receiver(args.db)

    def hook(index, phase, conn):
        if crash_at == (index, phase):
            # Simulate an abrupt kill: no cleanup, no commit, no reply.
            os._exit(CRASH_EXIT)

    out_fh = open(args.out, "a", encoding="utf-8")
    for i, op in enumerate(ops):
        result = run_op(receiver, op, index=i, hook=hook)
        # Queue advancement (if any) committed above. The reply is the only
        # thing still owed to the caller.
        hook(i, "post_commit", None)
        out_fh.write(json.dumps({"i": i, "op": op.get("op"), **result},
                                ensure_ascii=False) + "\n")
        out_fh.flush()
        os.fsync(out_fh.fileno())
    out_fh.close()
    receiver.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
