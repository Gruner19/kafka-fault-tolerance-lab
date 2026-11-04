#!/usr/bin/env python3
"""Harness do TP2: cria o tópico, lança atores, coleta evidência e verifica.

Fluxo por execução (seção 4.5 da spec):
  1. cria `r-events-<run_id>` com 1 partição e configuração explícita;
  2. lança observador e nós; espera READY de todos pelo canal de controle;
  3. libera os clientes com uma barreira de início conjunta;
  4. espera os clientes, drena o log e encerra os processos;
  5. grava `kafka_log.jsonl`, `meta.json`, `verification.json` e `summary.json`.

Uso:
    python3 tp2/src/harness/run_experiment.py --scenario tp2/src/harness/scenarios/normal.yaml
    python3 tp2/src/harness/run_experiment.py --scenario ... --preflight
"""

from __future__ import annotations

import argparse
import json
import platform
import socket
import subprocess
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

SRC = Path(__file__).resolve().parents[1]
TP2_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SRC))

import confluent_kafka  # noqa: E402

from common.config import ScenarioConfig, derive_seed, rng_for  # noqa: E402
from common.net import ReadyCollector, StartBarrierServer  # noqa: E402
from harness.admin import (  # noqa: E402
    create_topic,
    delete_topic,
    make_admin,
    verify_topic,
)
from harness.runtime import (  # noqa: E402
    dump_topic,
    free_port,
    read_jsonl,
    start_process,
    stop_process,
    wait_nodes_consumed,
    wait_port_ready,
)
from harness.verify_run import compute_waits, verify_run  # noqa: E402


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def git_commit() -> Optional[str]:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(TP2_ROOT.parent),
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def versions() -> dict:
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "confluent_kafka": str(confluent_kafka.version()),
        "librdkafka": str(confluent_kafka.libversion()),
    }


def build_clients(config: ScenarioConfig, seed: int) -> list[dict]:
    overrides = {
        str(item.get("client_id")): item for item in config.clients
    }
    clients = []
    for index in range(1, config.num_clients + 1):
        client_id = f"c{index}"
        override = overrides.get(client_id, {})
        rng = rng_for(seed, client_id)
        requests = int(
            override.get("requests", rng.randint(*config.requests_per_client))
        )
        think = override.get("think_time", config.think_time)
        think_pair = (float(think[0]), float(think[1]))
        node_index = int(override.get("node_index", (index - 1) % config.num_nodes + 1))
        clients.append({
            "client_id": client_id,
            "requests": requests,
            "think_time": think_pair,
            "node_index": node_index,
        })
    return clients


def preflight(config: ScenarioConfig, out_dir: Path) -> dict:
    checks: list[dict] = []

    def add(name: str, ok: bool, detail: Any) -> None:
        checks.append({"name": name, "ok": bool(ok), "detail": detail})

    admin = make_admin(config.bootstrap)
    try:
        metadata = admin.list_topics(timeout=10.0)
        brokers = [
            {"id": broker_id, "host": broker.host, "port": broker.port}
            for broker_id, broker in sorted(metadata.brokers.items())
        ]
        add("brokers", len(brokers) > 0, brokers)
    except Exception as exc:  # noqa: BLE001 - preflight reporta e segue
        add("brokers", False, str(exc))

    probe = f"tp2-preflight-{uuid.uuid4().hex[:8]}"
    try:
        created = create_topic(
            admin, probe,
            partitions=1,
            replication_factor=config.replication_factor,
            retention_ms=config.retention_ms,
            min_insync_replicas=config.min_insync_replicas,
        )
        info = verify_topic(admin, probe, 1)
        add("topic_single_partition", info["ok"], {
            "topic": probe,
            "partitions": len(info["partitions"]),
            "created": created.get("created"),
        })
        delete_topic(admin, probe)
        add("topic_delete", True, probe)
    except Exception as exc:  # noqa: BLE001
        add("topic_lifecycle", False, str(exc))

    add("python", sys.version_info >= (3, 10), versions())
    result = {"ok": all(check["ok"] for check in checks), "checks": checks}
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "preflight.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8",
    )
    return result


def summarize(config: ScenarioConfig, out_dir: Path, meta: dict, verification: dict) -> dict:
    node_logs = {
        path.stem.replace("node-", ""): read_jsonl(path)
        for path in sorted(out_dir.glob("node-*.jsonl"))
    }
    accesses = Counter()
    published: dict[str, int] = {}
    delivered: dict[str, int] = {}
    for process_id, records in node_logs.items():
        published[process_id] = sum(
            1 for record in records if record["event"] in ("ACQUIRE", "RELEASE")
        )
        for record in records:
            if record["event"] == "COMMITTED":
                accesses[str(record.get("client_id"))] += 1
            if record["event"] == "DELIVERED_DIGEST":
                delivered[process_id] = int(record["delivered"])

    requests_total = int(meta["requests_total"])
    total_published = sum(published.values())
    total_delivered = sum(delivered.values())
    observed = (
        (total_published + total_delivered) / requests_total
        if requests_total else None
    )
    return {
        "scenario": config.name,
        "run_id": meta["run_id"],
        "repetition": meta["repetition"],
        "seed": meta["seed"],
        "sync_enabled": config.sync_enabled,
        "capacity": config.capacity,
        "requests_total": requests_total,
        "completed": sum(accesses.values()),
        "duration_s": meta["duration_s"],
        "load_duration_s": meta["load_duration_s"],
        "waits_s": verification["metrics"]["waits_s"],
        "accesses_per_client": dict(sorted(accesses.items())),
        "cs_duration_s": list(config.cs_duration),
        "think_time_s": list(config.think_time),
        "protocol_published": published,
        "protocol_delivered": delivered,
        "messages_per_entry_observed": observed,
        "messages_per_entry_theoretical": 2 * config.num_nodes + 2,
        "topic": meta["topic"]["name"],
        "verification_ok": verification["ok"],
    }


def run_once(
    config: ScenarioConfig, args: argparse.Namespace, repetition: int,
) -> dict:
    run_seed = config.seed + repetition
    run_id = (
        f"{config.name}-{time.strftime('%Y%m%d-%H%M%S')}"
        f"-s{run_seed}-r{repetition + 1}-{uuid.uuid4().hex[:4]}"
    )
    topic = f"r-events-{run_id}"
    out_dir = Path(args.out) / config.name / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    clients = build_clients(config, run_seed)
    requests_total = sum(client["requests"] for client in clients)
    expected_messages = 2 * requests_total if config.sync_enabled else 0

    meta: dict = {
        "name": config.name,
        "run_id": run_id,
        "repetition": repetition + 1,
        "seed": run_seed,
        "num_nodes": config.num_nodes,
        "num_clients": config.num_clients,
        "sync_enabled": config.sync_enabled,
        "capacity": config.capacity,
        "expect_violation": config.expect_violation,
        "requests_total": requests_total,
        "clients": clients,
        "cs_duration": list(config.cs_duration),
        "think_time": list(config.think_time),
        "bootstrap": config.bootstrap,
        "replication_factor": config.replication_factor,
        "min_insync_replicas": config.min_insync_replicas,
        "retention_ms": config.retention_ms,
        "seeds": {
            "base": run_seed,
            "clients": {
                client["client_id"]: derive_seed(run_seed, client["client_id"])
                for client in clients
            },
            "nodes": {
                f"sync-{index}": derive_seed(run_seed, f"sync-{index}")
                for index in range(1, config.num_nodes + 1)
            },
        },
        "versions": versions(),
        "git_commit": git_commit(),
        "hostname": socket.gethostname(),
        "started_utc": utc_now(),
    }

    admin = make_admin(config.bootstrap)
    topic_created = create_topic(
        admin, topic,
        partitions=1,
        replication_factor=config.replication_factor,
        retention_ms=config.retention_ms,
        min_insync_replicas=config.min_insync_replicas,
    )
    topic_info = verify_topic(admin, topic, 1)
    if not topic_info["ok"]:
        raise RuntimeError(f"tópico {topic} não tem 1 partição: {topic_info}")
    meta["topic"] = {
        "name": topic,
        "partitions": len(topic_info["partitions"]),
        "replication_factor": config.replication_factor,
        "created": topic_created.get("created"),
        "configs": topic_info["configs"],
        "partition_detail": topic_info["partitions"],
    }

    observer_port = free_port()
    processes: list[subprocess.Popen] = []
    collector: Optional[ReadyCollector] = None
    barrier: Optional[StartBarrierServer] = None
    nodes: list[dict] = []

    try:
        processes.append(start_process("observer", [
            "-m", "observer.observer",
            "--port", str(observer_port),
            "--capacity", str(config.capacity),
            "--run-id", run_id,
            "--log-dir", str(out_dir),
        ], out_dir))
        if not wait_port_ready("127.0.0.1", observer_port, timeout=20.0):
            raise RuntimeError("observador não subiu")

        collector = ReadyCollector("127.0.0.1", 0, config.num_nodes)
        collector.serve_in_thread()

        for index in range(1, config.num_nodes + 1):
            port = free_port()
            nodes.append({"id": f"sync-{index}", "port": port})
            processes.append(start_process(f"node-{index}", [
                "-m", "sync_node.node",
                "--process-id", f"sync-{index}",
                "--port", str(port),
                "--bootstrap", config.bootstrap,
                "--topic", topic,
                "--run-id", run_id,
                "--log-dir", str(out_dir),
                "--control-port", str(collector.port),
                "--observer-port", str(observer_port),
                "--sync-enabled", "true" if config.sync_enabled else "false",
                "--capacity", str(config.capacity),
                "--cs-min", str(config.cs_duration[0]),
                "--cs-max", str(config.cs_duration[1]),
                "--seed", str(run_seed),
            ], out_dir))

        if not collector.wait(timeout=60.0):
            raise RuntimeError(f"READY incompleto: {collector.ready}")
        expected_ready = {node["id"] for node in nodes}
        if set(collector.ready) != expected_ready:
            raise RuntimeError(f"READY divergente: {collector.ready}")

        barrier = StartBarrierServer(
            "127.0.0.1", 0, len(clients), timeout=max(120.0, args.timeout),
        )
        barrier.serve_in_thread()

        response_timeout = max(
            120.0, requests_total * config.cs_duration[1] * 1.5 + 60.0,
        )
        client_processes: list[subprocess.Popen] = []
        for client in clients:
            node = nodes[client["node_index"] - 1]
            process = start_process(f"client-{client['client_id']}", [
                "-m", "client.client",
                "--client-id", client["client_id"],
                "--node-port", str(node["port"]),
                "--requests", str(client["requests"]),
                "--think-min", str(client["think_time"][0]),
                "--think-max", str(client["think_time"][1]),
                "--seed", str(run_seed),
                "--run-id", run_id,
                "--log-dir", str(out_dir),
                "--barrier-port", str(barrier.port),
                "--barrier-timeout", str(max(120.0, args.timeout)),
                "--response-timeout", str(response_timeout),
            ], out_dir)
            client_processes.append(process)
            processes.append(process)

        meta["load_started_utc"] = utc_now()
        for process in client_processes:
            code = process.wait(timeout=args.timeout)
            if code != 0:
                raise RuntimeError(f"cliente terminou com código {code}")
        meta["load_finished_utc"] = utc_now()

        if not wait_nodes_consumed(nodes, expected_messages, timeout=60.0):
            raise RuntimeError("nós não consumiram todo o log antes do encerramento")
    finally:
        for process in reversed(processes):
            stop_process(process)
        if barrier is not None:
            barrier.stop()
        if collector is not None:
            collector.stop()

    meta["finished_utc"] = utc_now()
    started = datetime.fromisoformat(meta["started_utc"].replace("Z", "+00:00"))
    finished = datetime.fromisoformat(meta["finished_utc"].replace("Z", "+00:00"))
    load_started = datetime.fromisoformat(meta["load_started_utc"].replace("Z", "+00:00"))
    load_finished = datetime.fromisoformat(meta["load_finished_utc"].replace("Z", "+00:00"))
    meta["duration_s"] = (finished - started).total_seconds()
    meta["load_duration_s"] = (load_finished - load_started).total_seconds()

    records = dump_topic(
        config.bootstrap, topic, expected_messages,
        out_dir / "kafka_log.jsonl", timeout=max(60.0, args.timeout),
    )
    meta["kafka_log_records"] = len(records)
    (out_dir / "meta.json").write_text(
        json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8",
    )

    verification = verify_run(out_dir, meta)
    (out_dir / "verification.json").write_text(
        json.dumps(verification, indent=2, ensure_ascii=False), encoding="utf-8",
    )
    summary = summarize(config, out_dir, meta, verification)
    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8",
    )

    meta["verification_ok"] = verification["ok"]
    (out_dir / "meta.json").write_text(
        json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8",
    )

    if not config.expect_violation and not verification["ok"]:
        failed = [check["name"] for check in verification["checks"] if not check["ok"]]
        raise RuntimeError(f"verificação falhou em {failed}; ver {out_dir}")

    if args.delete_topic:
        try:
            delete_topic(admin, topic)
        except Exception as exc:  # noqa: BLE001 - limpeza best-effort
            print(f"AVISO: falha ao apagar {topic}: {exc}", file=sys.stderr)

    return {
        "ok": verification["ok"],
        "out_dir": str(out_dir),
        "verification": verification,
        "summary": summary,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Harness de experimentos do TP2")
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--out", default=str(TP2_ROOT / "output"))
    parser.add_argument("--bootstrap")
    parser.add_argument("--repetitions", type=int)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--requests", type=int, help="fixa pedidos por cliente (dev)")
    parser.add_argument("--cs-min", type=float)
    parser.add_argument("--cs-max", type=float)
    parser.add_argument("--think-min", type=float)
    parser.add_argument("--think-max", type=float)
    parser.add_argument("--num-nodes", type=int)
    parser.add_argument("--num-clients", type=int)
    parser.add_argument("--capacity", type=int)
    parser.add_argument("--replication-factor", type=int)
    parser.add_argument("--min-insync-replicas", type=int)
    parser.add_argument("--delete-topic", action="store_true")
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--dry", action="store_true", help="preflight + criação/remoção de tópico")
    parser.add_argument("--timeout", type=float, default=900.0)
    return parser


def main(argv: Optional[list] = None) -> int:
    args = build_parser().parse_args(argv)
    config = ScenarioConfig.from_yaml(args.scenario)

    overrides: dict[str, Any] = {}
    if args.bootstrap:
        overrides["bootstrap"] = args.bootstrap
    if args.repetitions is not None:
        overrides["repetitions"] = args.repetitions
    if args.seed is not None:
        overrides["seed"] = args.seed
    if args.requests is not None:
        overrides["requests_per_client"] = (args.requests, args.requests)
    if args.cs_min is not None or args.cs_max is not None:
        low = args.cs_min if args.cs_min is not None else config.cs_duration[0]
        high = args.cs_max if args.cs_max is not None else config.cs_duration[1]
        overrides["cs_duration"] = (low, high)
    if args.think_min is not None or args.think_max is not None:
        low = args.think_min if args.think_min is not None else config.think_time[0]
        high = args.think_max if args.think_max is not None else config.think_time[1]
        overrides["think_time"] = (low, high)
    if args.num_nodes is not None:
        overrides["num_nodes"] = args.num_nodes
    if args.num_clients is not None:
        overrides["num_clients"] = args.num_clients
    if args.capacity is not None:
        overrides["capacity"] = args.capacity
    if args.replication_factor is not None:
        overrides["replication_factor"] = args.replication_factor
    if args.min_insync_replicas is not None:
        overrides["min_insync_replicas"] = args.min_insync_replicas
    if overrides:
        config = config.with_overrides(**overrides)

    if args.preflight or args.dry:
        result = preflight(config, Path(args.out) / config.name)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result["ok"] else 1

    results = []
    for repetition in range(config.repetitions):
        result = run_once(config, args, repetition)
        results.append(result)
        status = "PASS" if result["ok"] else "FAIL"
        print(f"[{status}] {result['summary']['run_id']} -> {result['out_dir']}", flush=True)

    summary = {
        "scenario": config.name,
        "runs": len(results),
        "passed": sum(1 for result in results if result["ok"]),
        "results": [
            {"run_id": result["summary"]["run_id"], "out_dir": result["out_dir"], "ok": result["ok"]}
            for result in results
        ],
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0 if all(result["ok"] for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
