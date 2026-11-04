#!/usr/bin/env python3
"""Verificador automático de uma execução do TP2 (V1–V8)."""

from __future__ import annotations

import hashlib
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Optional

ORDER = ["REQUEST", "ACQUIRE", "ENTER", "EXIT", "RELEASE", "COMMITTED"]


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


def _check(name: str, ok: bool, detail: Any) -> dict:
    return {"name": name, "ok": bool(ok), "detail": detail}


def _digest_offsets(offsets: list[int]) -> str:
    hasher = hashlib.sha256()
    for offset in offsets:
        hasher.update(f"{offset},".encode("ascii"))
    return hasher.hexdigest()


def compute_waits(node_logs: dict[str, list[dict]]) -> dict:
    values = []
    for records in node_logs.values():
        by_request: dict[str, dict[str, dict]] = defaultdict(dict)
        for record in records:
            if record["event"] in ("REQUEST", "ENTER"):
                by_request[record["request_id"]][record["event"]] = record
        for events in by_request.values():
            if "REQUEST" in events and "ENTER" in events:
                values.append(
                    (events["ENTER"]["mono_ns"] - events["REQUEST"]["mono_ns"]) / 1e9
                )
    if not values:
        return {"count": 0, "min": None, "median": None, "max": None}
    return {
        "count": len(values),
        "min": min(values),
        "median": statistics.median(values),
        "max": max(values),
    }


def verify_run(run_dir: str | Path, meta: dict) -> dict:
    """Executa V1–V8 e devolve o relatório de verificação."""
    run_dir = Path(run_dir)
    observer = read_jsonl(run_dir / "observer.jsonl")
    node_logs = {
        path.stem.replace("node-", ""): read_jsonl(path)
        for path in sorted(run_dir.glob("node-*.jsonl"))
    }
    client_logs = {
        path.stem.replace("client-", ""): read_jsonl(path)
        for path in sorted(run_dir.glob("client-*.jsonl"))
    }
    kafka_log = read_jsonl(run_dir / "kafka_log.jsonl")

    capacity = int(meta.get("capacity", 1))
    sync_enabled = bool(meta.get("sync_enabled", True))
    expect_violation = bool(meta.get("expect_violation", False))
    requests_total = int(meta.get("requests_total", 0))

    checks: list[dict] = []

    # V1 — exclusão mútua (ou detecção de sobreposição no modo control).
    summary = next(
        (record for record in reversed(observer) if record["event"] == "SUMMARY"), {},
    )
    max_active = int(summary.get("max_active", 0))
    overlaps = int(summary.get("overlaps", 0))
    if expect_violation:
        checks.append(_check(
            "V1",
            overlaps > 0,
            {"max_active": max_active, "overlaps": overlaps, "expected_violation": True},
        ))
    else:
        checks.append(_check(
            "V1",
            max_active <= capacity and overlaps == 0,
            {"max_active": max_active, "overlaps": overlaps, "capacity": capacity},
        ))

    # V2 — completude: cada request_id pedido tem exatamente um COMMITTED.
    requested = Counter()
    committed_clients = Counter()
    for records in client_logs.values():
        for record in records:
            if record["event"] == "REQUEST":
                requested[record["request_id"]] += 1
            elif record["event"] == "COMMITTED":
                committed_clients[record["request_id"]] += 1
    committed_nodes = Counter()
    for records in node_logs.values():
        for record in records:
            if record["event"] == "COMMITTED":
                committed_nodes[record["request_id"]] += 1

    missing = sorted(set(requested) - set(committed_clients))
    duplicated = sorted(
        rid for rid, count in committed_clients.items() if count != 1
    )
    v2_ok = (
        not missing
        and not duplicated
        and set(committed_clients) == set(committed_nodes)
        and (requests_total == 0 or len(requested) == requests_total)
        and all(count == 1 for count in requested.values())
    )
    checks.append(_check("V2", v2_ok, {
        "requested": len(requested),
        "committed_clients": len(committed_clients),
        "committed_nodes": len(committed_nodes),
        "requests_total": requests_total,
        "missing": missing[:10],
        "duplicated": duplicated[:10],
    }))

    # V3 — nenhum request_id com mais de um ENTER.
    enters = Counter()
    for records in node_logs.values():
        for record in records:
            if record["event"] == "ENTER":
                enters[record["request_id"]] += 1
    duplicated_enters = sorted(rid for rid, count in enters.items() if count > 1)
    checks.append(_check("V3", not duplicated_enters, {
        "enters": len(enters), "duplicated": duplicated_enters[:10],
    }))

    # V4 — ordem dos BEGIN == ordem crescente dos offsets de ACQUIRE.
    if sync_enabled and not expect_violation:
        acquires = []
        for records in node_logs.values():
            for record in records:
                if record["event"] == "ACQUIRE":
                    acquires.append((int(record["offset"]), record["request_id"]))
        acquires.sort()
        begins = sorted(
            (record for record in observer if record["event"] == "BEGIN"),
            key=lambda record: record["obs_seq"],
        )
        order_begin = [rid for _, rid in acquires]
        order_log = [record["request_id"] for record in begins]
        checks.append(_check("V4", order_begin == order_log, {
            "acquires": len(order_begin),
            "begins": len(order_log),
            "first_mismatch": next(
                (
                    index for index, (left, right) in enumerate(zip(order_begin, order_log))
                    if left != right
                ),
                None,
            ),
        }))
    else:
        checks.append(_check("V4", True, {"skipped": "modo control"}))

    # V5 — todos os consumidores leram a mesma sequência.
    digests = {}
    delivered = {}
    for process_id, records in node_logs.items():
        digest_record = next(
            (record for record in reversed(records) if record["event"] == "DELIVERED_DIGEST"),
            None,
        )
        if digest_record is not None:
            digests[process_id] = digest_record["sha256"]
            delivered[process_id] = int(digest_record["delivered"])
    expected_digest: Optional[str] = None
    kafka_ok = True
    if kafka_log:
        offsets = [int(record["offset"]) for record in kafka_log]
        expected_digest = _digest_offsets(offsets)
        kafka_ok = offsets == list(range(len(offsets)))
    ready_offsets = {}
    for process_id, records in node_logs.items():
        ready = next(
            (record for record in records if record["event"] == "READY"), None,
        )
        if ready is not None and ready.get("start_offset") is not None:
            ready_offsets[process_id] = int(ready["start_offset"])
    readiness_known = len(ready_offsets) == len(node_logs) and bool(node_logs)
    readiness_ok = (not readiness_known) or all(
        offset == 0 for offset in ready_offsets.values()
    )
    v5_ok = (
        len(digests) == len(node_logs)
        and len(set(digests.values())) == 1
        and len(set(delivered.values())) == 1
        and kafka_ok
        and readiness_ok
        and (expected_digest is None or expected_digest in set(digests.values()))
    )
    checks.append(_check("V5", v5_ok, {
        "nodes": len(digests),
        "delivered": delivered,
        "digests_equal": len(set(digests.values())) == 1,
        "kafka_log_records": len(kafka_log),
        "kafka_offsets_contiguous": kafka_ok,
        "expected_digest": expected_digest,
        "ready_start_offsets": ready_offsets,
        "readiness_known": readiness_known,
    }))

    # V6 — invariantes e RELEASE sempre removendo a cabeça.
    violations = sum(
        1 for records in node_logs.values() for record in records
        if record["event"] == "INVARIANT_VIOLATION"
    )
    bad_release = []
    for process_id, records in node_logs.items():
        for record in records:
            if record["event"] == "RELEASE" and record.get("queue_head") != record["request_id"]:
                bad_release.append((process_id, record["request_id"], record.get("queue_head")))
    checks.append(_check("V6", violations == 0 and not bad_release, {
        "invariant_violations": violations,
        "release_not_head": bad_release[:10],
    }))

    # V7 — tópico com uma única partição, sem compactação e com RF/ISR pedidas.
    topic = meta.get("topic", {})
    configs = topic.get("configs", {}) or {}
    cleanup = str(configs.get("cleanup.policy", "")).lower()
    expected_isr = meta.get("min_insync_replicas")
    isr_ok = (
        expected_isr is None
        or str(configs.get("min.insync.replicas")) == str(expected_isr)
    )
    expected_rf = int(topic.get("replication_factor", 0) or 0)
    partition_detail = topic.get("partition_detail") or []
    replicas_ok = (
        not partition_detail
        or all(
            len(partition.get("replicas", [])) == expected_rf
            for partition in partition_detail
        )
    )
    v7_ok = (
        int(topic.get("partitions", 0)) == 1
        and "compact" not in cleanup
        and isr_ok
        and replicas_ok
    )
    checks.append(_check("V7", v7_ok, {
        **topic,
        "cleanup_delete": "compact" not in cleanup,
        "isr_ok": isr_ok,
        "replicas_ok": replicas_ok,
    }))

    # V8 — espera máxima observada vs. cota teórica (informativo).
    waits = compute_waits(node_logs)
    cs_max = float(meta.get("cs_duration", [0, 0])[1])
    bound = cs_max * max(1, requests_total - 1)
    v8_ok = waits["max"] is None or waits["max"] <= bound * 1.5
    checks.append(_check("V8", v8_ok, {
        "informative": True,
        "observed_max_s": waits["max"],
        "theoretical_bound_s": bound,
    }))

    ok = all(check["ok"] for check in checks)
    return {
        "ok": ok,
        "scenario": meta.get("name"),
        "run_id": meta.get("run_id"),
        "checks": checks,
        "metrics": {
            "waits_s": waits,
            "max_active": max_active,
            "overlaps": overlaps,
        },
    }


def main(argv: Optional[list] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Verificador V1–V8 do TP2")
    parser.add_argument("run_dir")
    args = parser.parse_args(argv)
    meta = json.loads((Path(args.run_dir) / "meta.json").read_text(encoding="utf-8"))
    report = verify_run(args.run_dir, meta)
    (Path(args.run_dir) / "verification.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8",
    )
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
