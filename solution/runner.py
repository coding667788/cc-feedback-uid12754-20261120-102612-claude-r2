"""Shared op-runner used by the CLI and the crash-test worker."""

from __future__ import annotations


def run_op(receiver, op, index=None, hook=None):
    """Run one op dict against a Receiver.

    ``hook`` (optional) is called as ``hook(index, phase, conn)``; the only
    phase emitted from inside an op is ``"pre_commit"`` (all writes done,
    COMMIT not yet issued) for ``recv`` ops.
    """
    kind = op.get("op")
    if kind == "establish":
        return receiver.establish(op["session"], op["tenant"],
                                  op["conn_gen"], op["base_seq"])
    if kind == "key":
        return receiver.add_key(op["session"], op["key_gen"],
                                op["lo"], op.get("hi"))
    if kind == "recv":
        before = None
        if hook is not None:
            def before(conn, _i=index):
                hook(_i, "pre_commit", conn)
        return receiver.recv(op["session"], op["seq"], op["conn_gen"],
                             op["key_gen"], op["digest"],
                             auth=op.get("auth", True),
                             tenant=op.get("tenant"),
                             before_commit=before)
    if kind == "revoke":
        return receiver.revoke(op["session"], op["key_gen"])
    raise ValueError(f"unknown op: {kind!r}")


def run_ops(receiver, ops, hook=None):
    return [run_op(receiver, op, index=i, hook=hook)
            for i, op in enumerate(ops)]
