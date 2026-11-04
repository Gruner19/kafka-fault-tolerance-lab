"""Teste do logger JSONL usado como evidência."""

from __future__ import annotations

import json

import pytest

from common.jsonl_log import JsonlLogger


def test_jsonl_logger_writes_base_and_extra_fields(tmp_path) -> None:
    path = tmp_path / "node-sync-1.jsonl"
    with JsonlLogger(path, actor="sync-1", run_id="run-1") as logger:
        logger.log("ENTER", request_id="c1-0001", offset=4, queue_len=2)

    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert record["actor"] == "sync-1"
    assert record["run_id"] == "run-1"
    assert record["event"] == "ENTER"
    assert record["request_id"] == "c1-0001"
    assert record["offset"] == 4
    assert record["ts_utc"].endswith("Z")
    assert isinstance(record["mono_ns"], int)


def test_jsonl_logger_rejects_reserved_fields(tmp_path) -> None:
    with JsonlLogger(tmp_path / "x.jsonl", actor="a") as logger:
        with pytest.raises(ValueError):
            logger.log("ENTER", actor="outro")
