"""CLI entry point: python -m solution --input cases.json --output result.json

Runs every case in the input file against a fresh SQLite database (fully
offline, no network) and writes the per-op results plus the final queue,
gaps and session state to the output file.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile

from .core import Receiver
from .runner import run_ops


def run_case(case, workdir):
    name = case.get("name", "unnamed")
    db_path = os.path.join(workdir, f"case_{abs(hash(name)) & 0xFFFFFFFF:x}.db")
    receiver = Receiver(db_path)
    try:
        ops = case.get("ops", [])
        op_results = run_ops(receiver, ops)
        sessions = sorted({op["session"] for op in ops if "session" in op})
        out_sessions = {}
        for sid in sessions:
            state = receiver.session_state(sid)
            out_sessions[sid] = {
                "state": state,
                "delivered": receiver.delivered(sid),
                "gaps": receiver.gaps(sid),
                "cached": receiver.cached(sid),
            }
        return {
            "name": name,
            "ops": [dict({"i": i, "op": op.get("op")}, **res)
                    for i, (op, res) in enumerate(zip(ops, op_results))],
            "sessions": out_sessions,
        }
    finally:
        receiver.close()


def main(argv=None):
    parser = argparse.ArgumentParser(prog="solution",
                                     description="Offline receiver simulator")
    parser.add_argument("--input", required=True, help="cases.json path")
    parser.add_argument("--output", required=True, help="result.json path")
    args = parser.parse_args(argv)

    with open(args.input, "r", encoding="utf-8") as fh:
        spec = json.load(fh)

    workdir = tempfile.mkdtemp(prefix="receiver_cases_")
    results = [run_case(case, workdir) for case in spec.get("cases", [])]

    payload = {"input": os.path.basename(args.input), "cases": results}
    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False, sort_keys=False)
        fh.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
