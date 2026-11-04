"""Escrita de evidência em JSON Lines, um arquivo por ator."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Optional

from common.clock import mono_ns, utc_iso

_RESERVED = frozenset({"ts_utc", "mono_ns", "actor", "event", "run_id"})


class JsonlLogger:
    """Logger append-only e thread-safe; cada linha é descarregada no arquivo."""

    def __init__(self, path: str | Path, actor: str, run_id: Optional[str] = None) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.actor = actor
        self.run_id = run_id
        self._lock = threading.Lock()
        self._fh = self.path.open("a", encoding="utf-8")

    def log(self, event: str, **fields: Any) -> dict:
        reserved = _RESERVED.intersection(fields)
        if reserved:
            raise ValueError(f"campos reservados no evento: {sorted(reserved)}")

        record: dict[str, Any] = {
            "ts_utc": utc_iso(),
            "mono_ns": mono_ns(),
            "actor": self.actor,
            "event": event,
        }
        if self.run_id is not None:
            record["run_id"] = self.run_id
        record.update(fields)

        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            self._fh.write(line + "\n")
            self._fh.flush()
        return record

    def close(self) -> None:
        with self._lock:
            if not self._fh.closed:
                self._fh.close()

    def __enter__(self) -> "JsonlLogger":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()
