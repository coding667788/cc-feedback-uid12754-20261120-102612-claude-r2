"""Fully offline communication receiver simulator.

Synthetic message metadata + simulated authentication + local SQLite.
No network listening anywhere.
"""

from .core import (
    Receiver,
    ConcurrencyError,
    ERR,
    WINDOW,
    MOD,
    recover_abs_seq,
)

__all__ = ["Receiver", "ConcurrencyError", "ERR", "WINDOW", "MOD", "recover_abs_seq"]
