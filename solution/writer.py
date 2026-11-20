"""Independent writer process used by the concurrency / auth tests.

    python -m solution.writer --db DB --session S --conn-gen N \
        --wire W --key-gen K --digest D [--auth 0|1] \
        [--attempts N] [--delay-ms MS]

Prints one JSON line per attempt to stdout. Multiple writer processes can
target the same database file concurrently; SQLite's BEGIN IMMEDIATE plus
the in-transaction version check serialize them.
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from .core import Receiver


def main(argv=None):
    parser = argparse.ArgumentParser(prog="solution.writer")
    parser.add_argument("--db", required=True)
    parser.add_argument("--session", required=True)
    parser.add_argument("--conn-gen", type=int, required=True)
    parser.add_argument("--wire", required=True,
                        help="wire sequence, or comma-separated list")
    parser.add_argument("--key-gen", type=int, required=True)
    parser.add_argument("--digest", required=True,
                        help="digest; with a wire list, ':' + wire is appended")
    parser.add_argument("--auth", type=int, default=1)
    parser.add_argument("--attempts", type=int, default=1)
    parser.add_argument("--delay-ms", type=float, default=0.0)
    args = parser.parse_args(argv)

    wires = [int(x) for x in args.wire.split(",")]
    receiver = Receiver(args.db)
    try:
        for _ in range(args.attempts):
            for w in wires:
                digest = args.digest if len(wires) == 1 else f"{args.digest}:{w}"
                res = receiver.recv(args.session, w, args.conn_gen,
                                    args.key_gen, digest,
                                    auth=bool(args.auth))
                print(json.dumps(res, ensure_ascii=False), flush=True)
            if args.delay_ms:
                time.sleep(args.delay_ms / 1000.0)
    finally:
        receiver.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
