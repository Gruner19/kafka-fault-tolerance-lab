"""Relógios do TP2: monotônico para esperas e UTC apenas informativo."""

from __future__ import annotations

import time
from datetime import datetime, timezone


def mono_ns() -> int:
    """Instante monotônico em nanossegundos (comparável só no mesmo processo)."""
    return time.monotonic_ns()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso() -> str:
    """Timestamp UTC com milissegundos, sufixo Z (informativo)."""
    return utc_now().isoformat(timespec="milliseconds").replace("+00:00", "Z")
