#!/usr/bin/env python3
"""Administração explícita de tópicos do TP2.

Nunca depende de auto-criação de tópicos: o harness cria o tópico com uma única
partição e verifica a configuração antes de iniciar a carga.

Uso:
    python3 tp2/src/harness/admin.py create \
        --topic r-events-dev --partitions 1 --replication-factor 1
    python3 tp2/src/harness/admin.py describe --topic r-events-dev
    python3 tp2/src/harness/admin.py verify --topic r-events-dev --partitions 1
    python3 tp2/src/harness/admin.py delete --topic r-events-dev
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Optional

from confluent_kafka.admin import (
    RESOURCE_TOPIC,
    AdminClient,
    ConfigResource,
    NewTopic,
)

DEFAULT_BOOTSTRAP = "localhost:9092"
DEFAULT_RETENTION_MS = 86_400_000


def make_admin(bootstrap: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap, "client.id": "tp2-admin"})


def _topic_metadata(admin: AdminClient, topic: str):
    return admin.list_topics(topic=topic, timeout=10.0).topics.get(topic)


def topic_exists(admin: AdminClient, topic: str) -> bool:
    metadata = admin.list_topics(timeout=10.0).topics.get(topic)
    return metadata is not None and metadata.error is None


def wait_topic(admin: AdminClient, topic: str, timeout: float = 15.0) -> bool:
    """Espera a metadata do tópico ficar visível com partições."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        metadata = _topic_metadata(admin, topic)
        if metadata is not None and metadata.error is None and metadata.partitions:
            return True
        time.sleep(0.2)
    return False


def create_topic(
    admin: AdminClient,
    topic: str,
    partitions: int = 1,
    replication_factor: int = 1,
    retention_ms: int = DEFAULT_RETENTION_MS,
    min_insync_replicas: Optional[int] = None,
) -> dict:
    if topic_exists(admin, topic):
        return {"topic": topic, "created": False, "reason": "already exists"}

    config = {
        "retention.ms": str(retention_ms),
        "cleanup.policy": "delete",
    }
    if min_insync_replicas is not None:
        config["min.insync.replicas"] = str(min_insync_replicas)

    new_topic = NewTopic(
        topic,
        num_partitions=partitions,
        replication_factor=replication_factor,
        config=config,
    )
    futures = admin.create_topics(
        [new_topic], operation_timeout=15.0, request_timeout=15.0,
    )
    futures[topic].result(timeout=20.0)

    if not wait_topic(admin, topic):
        raise RuntimeError(f"tópico {topic!r} não ficou visível na metadata")

    return {
        "topic": topic,
        "created": True,
        "partitions": partitions,
        "replication_factor": replication_factor,
        "config": config,
    }


def describe_topic_configs(admin: AdminClient, topic: str) -> dict:
    resource = ConfigResource(RESOURCE_TOPIC, topic)
    futures = admin.describe_configs([resource], request_timeout=10.0)
    entries = futures[resource].result(timeout=15.0)
    return {name: entry.value for name, entry in entries.items()}


def describe_topic(admin: AdminClient, topic: str) -> dict:
    metadata = _topic_metadata(admin, topic)
    if metadata is None or metadata.error is not None:
        detail = "sem metadata" if metadata is None else str(metadata.error)
        raise RuntimeError(f"tópico {topic!r} não encontrado ({detail})")

    partitions = []
    for partition_id, partition in sorted(metadata.partitions.items()):
        partitions.append({
            "id": partition_id,
            "leader": partition.leader,
            "replicas": list(partition.replicas),
            "isr": list(partition.isrs),
        })

    return {
        "topic": topic,
        "partitions": partitions,
        "configs": describe_topic_configs(admin, topic),
    }


def verify_topic(admin: AdminClient, topic: str, expected_partitions: int) -> dict:
    info = describe_topic(admin, topic)
    observed = len(info["partitions"])
    return {
        "topic": topic,
        "expected_partitions": expected_partitions,
        "observed_partitions": observed,
        "ok": observed == expected_partitions,
        "partitions": info["partitions"],
        "configs": info["configs"],
    }


def delete_topic(admin: AdminClient, topic: str) -> dict:
    futures = admin.delete_topics([topic], operation_timeout=15.0)
    futures[topic].result(timeout=20.0)
    return {"topic": topic, "deleted": True}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Administração de tópicos do TP2")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create", help="cria o tópico explicitamente")
    create.add_argument("--topic", required=True)
    create.add_argument("--partitions", type=int, default=1)
    create.add_argument("--replication-factor", type=int, default=1)
    create.add_argument("--retention-ms", type=int, default=DEFAULT_RETENTION_MS)
    create.add_argument("--min-insync-replicas", type=int, default=None)

    describe = sub.add_parser("describe", help="mostra partições e configuração")
    describe.add_argument("--topic", required=True)

    verify = sub.add_parser("verify", help="falha se o número de partições divergir")
    verify.add_argument("--topic", required=True)
    verify.add_argument("--partitions", type=int, default=1)

    delete = sub.add_parser("delete", help="remove o tópico")
    delete.add_argument("--topic", required=True)

    return parser


def main(argv: Optional[list] = None) -> int:
    args = _build_parser().parse_args(argv)
    admin = make_admin(args.bootstrap)

    try:
        if args.command == "create":
            result = create_topic(
                admin,
                args.topic,
                partitions=args.partitions,
                replication_factor=args.replication_factor,
                retention_ms=args.retention_ms,
                min_insync_replicas=args.min_insync_replicas,
            )
        elif args.command == "describe":
            result = describe_topic(admin, args.topic)
        elif args.command == "verify":
            result = verify_topic(admin, args.topic, args.partitions)
        else:
            result = delete_topic(admin, args.topic)
    except Exception as exc:  # noqa: BLE001 - CLI: reportar e sair
        print(f"ERRO: {exc}", file=sys.stderr)
        return 2

    print(json.dumps(result, indent=2, ensure_ascii=False))
    if args.command == "verify" and not result["ok"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
