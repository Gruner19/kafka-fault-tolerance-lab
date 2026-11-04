#!/usr/bin/env python3
"""Demonstração local da fase 3: observador + N nós + N clientes + verificação.

Sobe processos separados (um por ator), usa o Kafka de desenvolvimento e
confere a ordem 4.3, a exclusão mútua no observador e a igualdade do log
consumido pelos nós. Não substitui o harness da fase 4 (não cria evidência
`kafka_log.jsonl` nem roda `verify_run.py`).

Uso:
    python3 tp2/scripts/demo_local.py
    python3 tp2/scripts/demo_local.py --no-sync
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Optional

TP2_ROOT = Path(__file__).resolve().parents[1]
SRC = TP2_ROOT / "src"
sys.path.insert(0, str(SRC))

from harness.admin import create_topic, make_admin, verify_topic  # noqa: E402

PYTHON = sys.executable
ENV = os.environ.copy()
ENV["PYTHONPATH"] = str(SRC) + os.pathsep + ENV.get("PYTHONPATH", "")

ORDER = ["REQUEST", "ACQUIRE", "ENTER", "EXIT", "RELEASE", "COMMITTED"]


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


def start_process(name: str, args: list[str], out_dir: Path) -> subprocess.Popen:
    stdout_path = out_dir / f"{name}.out.log"
    stderr_path = out_dir / f"{name}.err.log"
    stdout = stdout_path.open("w", encoding="utf-8")
    stderr = stderr_path.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        [PYTHON, *args],
        cwd=str(TP2_ROOT),
        env=ENV,
        stdout=stdout,
        stderr=stderr,
        text=True,
    )
    proc._tp2_logs = (stdout, stderr)  # type: ignore[attr-defined]
    return proc


def stop_process(proc: subprocess.Popen, timeout: float = 10.0) -> int:
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
    try:
        code = proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        code = proc.wait(timeout=5)
    for handle in getattr(proc, "_tp2_logs", ()):
        handle.close()
    return code


def wait_nodes_ready(nodes: list[dict], expected: int, timeout: float = 45.0) -> bool:
    from common.net import connect, wait_port

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        delivered = []
        for node in nodes:
            try:
                if not wait_port("127.0.0.1", node["port"], timeout=0.2):
                    break
                connection = connect("127.0.0.1", node["port"], timeout=2.0)
                connection.send({"type": "STATUS"})
                response = connection.recv(timeout=3.0)
                connection.close()
                delivered.append(int(response["delivered"]))
            except (OSError, TimeoutError, KeyError):
                break
        else:
            if len(delivered) == len(nodes) and all(value >= expected for value in delivered):
                return True
        time.sleep(0.1)
    return False


def verify(out_dir: Path, node_ids: list[str], client_ids: list[str], sync_enabled: bool) -> dict:
    node_logs = {pid: read_jsonl(out_dir / f"node-{pid}.jsonl") for pid in node_ids}
    client_logs = {cid: read_jsonl(out_dir / f"client-{cid}.jsonl") for cid in client_ids}
    observer = read_jsonl(out_dir / "observer.jsonl")

    checks: dict[str, bool] = {}
    details: dict[str, object] = {}

    # V2/V3: cada pedido tem exatamente um REQUEST/COMMITTED no cliente.
    client_requests = 0
    for client_id, records in client_logs.items():
        requests = [r for r in records if r["event"] == "REQUEST"]
        committed = [r for r in records if r["event"] == "COMMITTED"]
        client_requests += len(requests)
        checks[f"client_{client_id}_complete"] = (
            len(requests) == len(committed)
            and {r["request_id"] for r in requests} == {r["request_id"] for r in committed}
        )
        checks[f"client_{client_id}_no_errors"] = not any(
            r["event"] == "ERROR" for r in records
        )
    details["client_requests"] = client_requests

    # Ordem 4.3 por nó, usando mono_ns do mesmo processo.
    required = ORDER if sync_enabled else [
        name for name in ORDER if name not in ("ACQUIRE", "RELEASE")
    ]
    order_ok = True
    for process_id, records in node_logs.items():
        by_request: dict[str, dict[str, dict]] = {}
        for record in records:
            if record.get("process_id") != process_id:
                continue
            if record["event"] in required:
                by_request.setdefault(record["request_id"], {})[record["event"]] = record
        for request_id, events in by_request.items():
            missing = [name for name in required if name not in events]
            if missing:
                order_ok = False
                details.setdefault("missing_events", []).append((request_id, missing))
                continue
            stamps = [events[name]["mono_ns"] for name in required]
            if stamps != sorted(stamps):
                order_ok = False
                details.setdefault("bad_order", []).append(request_id)
    checks["node_event_order"] = order_ok

    # V4: ordem dos BEGIN == ordem crescente dos offsets de ACQUIRE.
    if sync_enabled:
        acquires = []
        for records in node_logs.values():
            for record in records:
                if record["event"] == "ACQUIRE":
                    acquires.append((int(record["offset"]), record["request_id"]))
        acquires.sort()
        begins = [
            record for record in observer
            if record["event"] == "BEGIN"
        ]
        begins.sort(key=lambda record: record["obs_seq"])
        checks["begin_order_matches_offsets"] = [rid for _, rid in acquires] == [
            record["request_id"] for record in begins
        ]
        details["acquires"] = len(acquires)
        details["begins"] = len(begins)

    # V1: observador.
    observer_summary = next(
        (record for record in reversed(observer) if record["event"] == "SUMMARY"), {},
    )
    max_active = int(observer_summary.get("max_active", 0))
    overlaps = int(observer_summary.get("overlaps", 0))
    details["max_active"] = max_active
    details["overlaps"] = overlaps
    if sync_enabled:
        checks["mutual_exclusion"] = max_active <= 1 and overlaps == 0
    else:
        checks["control_detects_overlap"] = overlaps > 0

    # V5: todos os nós consumiram a mesma sequência (digest).
    digests = {}
    delivered = {}
    for process_id, records in node_logs.items():
        digest = next(
            (r for r in reversed(records) if r["event"] == "DELIVERED_DIGEST"), None,
        )
        if digest is not None:
            digests[process_id] = digest["sha256"]
            delivered[process_id] = int(digest["delivered"])
    checks["identical_consumed_log"] = len(set(digests.values())) == 1 and len(digests) == len(node_ids)
    details["digests"] = digests
    details["delivered"] = delivered

    # V6: nenhuma violação de invariante nos nós.
    checks["no_invariant_violations"] = all(
        not any(r["event"] == "INVARIANT_VIOLATION" for r in records)
        for records in node_logs.values()
    )

    return {"ok": all(checks.values()), "checks": checks, "details": details}


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="Demo local da fase 3")
    parser.add_argument("--clients", type=int, default=3)
    parser.add_argument("--requests", type=int, default=5)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--out", default=str(TP2_ROOT / "output" / "fase3"))
    parser.add_argument("--no-sync", action="store_true")
    parser.add_argument("--cs-min", type=float, default=0.05)
    parser.add_argument("--cs-max", type=float, default=0.2)
    parser.add_argument("--think-min", type=float, default=0.05)
    parser.add_argument("--think-max", type=float, default=0.1)
    parser.add_argument("--seed", type=int, default=2026)
    args = parser.parse_args(argv)

    run_id = args.run_id or f"demo-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:4]}"
    out_dir = Path(args.out) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    sync_enabled = not args.no_sync
    topic = f"r-events-{run_id}"
    admin = make_admin(args.bootstrap)
    create_topic(
        admin, topic,
        partitions=1,
        replication_factor=1,
        min_insync_replicas=1,
    )
    verified = verify_topic(admin, topic, 1)
    if not verified["ok"]:
        print(json.dumps(verified, indent=2, ensure_ascii=False))
        return 2

    observer_port = free_port()
    processes: list[subprocess.Popen] = []
    collector = None
    barrier = None

    try:
        processes.append(start_process("observer", [
            "-m", "observer.observer",
            "--port", str(observer_port),
            "--capacity", "1",
            "--run-id", run_id,
            "--log-dir", str(out_dir),
        ], out_dir))

        from common.net import ReadyCollector, StartBarrierServer, wait_port

        if not wait_port("127.0.0.1", observer_port, timeout=15.0):
            raise RuntimeError("observador não subiu")

        collector = ReadyCollector("127.0.0.1", 0, args.clients)
        collector.serve_in_thread()

        nodes = []
        for index in range(1, args.clients + 1):
            port = free_port()
            nodes.append({"id": f"sync-{index}", "port": port})
            processes.append(start_process(f"node-{index}", [
                "-m", "sync_node.node",
                "--process-id", f"sync-{index}",
                "--port", str(port),
                "--bootstrap", args.bootstrap,
                "--topic", topic,
                "--run-id", run_id,
                "--log-dir", str(out_dir),
                "--control-port", str(collector.port),
                "--observer-port", str(observer_port),
                "--sync-enabled", "true" if sync_enabled else "false",
                "--cs-min", str(args.cs_min),
                "--cs-max", str(args.cs_max),
                "--seed", str(args.seed),
            ], out_dir))

        if not collector.wait(timeout=45.0):
            raise RuntimeError(f"nós não enviaram READY: {collector.ready}")
        ready = set(collector.ready)
        expected_ready = {node["id"] for node in nodes}
        if ready != expected_ready:
            raise RuntimeError(f"READY incompleto: {ready} != {expected_ready}")

        barrier = StartBarrierServer("127.0.0.1", 0, args.clients, timeout=120.0)
        barrier.serve_in_thread()

        clients: list[subprocess.Popen] = []
        for index, node in enumerate(nodes, start=1):
            client = start_process(f"client-{index}", [
                "-m", "client.client",
                "--client-id", f"c{index}",
                "--node-port", str(node["port"]),
                "--requests", str(args.requests),
                "--think-min", str(args.think_min),
                "--think-max", str(args.think_max),
                "--seed", str(args.seed),
                "--run-id", run_id,
                "--log-dir", str(out_dir),
                "--barrier-port", str(barrier.port),
            ], out_dir)
            clients.append(client)
            processes.append(client)

        for client in clients:
            code = client.wait(timeout=180)
            if code != 0:
                raise RuntimeError(f"cliente terminou com código {code}")

        expected_messages = 2 * args.clients * args.requests if sync_enabled else 0
        if not wait_nodes_ready(nodes, expected_messages, timeout=45.0):
            raise RuntimeError("nós não consumiram todo o log antes do encerramento")
    finally:
        for proc in reversed(processes):
            stop_process(proc)
        if barrier is not None:
            barrier.stop()
        if collector is not None:
            collector.stop()

    result = verify(out_dir, [f"sync-{i}" for i in range(1, args.clients + 1)],
                    [f"c{i}" for i in range(1, args.clients + 1)], sync_enabled)
    result.update({
        "run_id": run_id,
        "topic": topic,
        "sync_enabled": sync_enabled,
        "out_dir": str(out_dir),
        "requests_per_client": args.requests,
    })
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
