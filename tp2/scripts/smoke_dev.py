#!/usr/bin/env python3
"""Smoke do ambiente de desenvolvimento do TP2.

Cria/verifica o tópico com 1 partição, publica um par ACQUIRE/RELEASE com
`key="R"` na partição 0 e relê o log desde o offset 0 com um consumidor
próprio (`assign` + `seek`), comprovando offsets crescentes e payload JSON.

Uso:
    python3 tp2/scripts/smoke_dev.py [--topic r-events-smoke] [--bootstrap localhost:9092]

Saída: JSON com o resultado. Exit code 0 se o smoke passou.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC))

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition  # noqa: E402
from harness.admin import create_topic, make_admin, verify_topic  # noqa: E402


def produce_pair(topic: str, bootstrap: str, run_id: str) -> list:
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "tp2-smoke-producer",
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 0,
        "message.timeout.ms": 15000,
    })

    produced = []
    errors = []

    def on_delivery(err, msg) -> None:
        if err is not None:
            errors.append(str(err))
            return
        produced.append({
            "partition": msg.partition(),
            "offset": msg.offset(),
            "key": msg.key().decode("utf-8") if msg.key() else None,
            "value": msg.value().decode("utf-8") if msg.value() else None,
        })

    payloads = [
        {
            "type": "ACQUIRE",
            "resource": "R",
            "process_id": "smoke-1",
            "request_id": f"smoke-{run_id}",
        },
        {
            "type": "RELEASE",
            "resource": "R",
            "process_id": "smoke-1",
            "request_id": f"smoke-{run_id}",
        },
    ]

    for payload in payloads:
        producer.produce(
            topic,
            key=b"R",
            value=json.dumps(payload).encode("utf-8"),
            partition=0,
            on_delivery=on_delivery,
        )
    pending = producer.flush(15.0)
    if pending:
        errors.append(f"{pending} mensagem(ns) não entregue(s)")
    if errors:
        raise RuntimeError("; ".join(errors))
    if len(produced) != 2:
        raise RuntimeError(f"esperava 2 delivery reports, obtive {len(produced)}")
    return produced


def consume_until(topic: str, bootstrap: str, offsets: set, timeout: float = 15.0) -> list:
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"tp2-smoke-{uuid.uuid4().hex}",
        "client.id": "tp2-smoke-consumer",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "enable.partition.eof": True,
    })
    consumer.assign([TopicPartition(topic, 0, 0)])

    seen = []
    found = set()
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline and not offsets.issubset(found):
            msg = consumer.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())
            seen.append({
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key().decode("utf-8") if msg.key() else None,
                "value": msg.value().decode("utf-8") if msg.value() else None,
            })
            found.add(msg.offset())
    finally:
        consumer.close()
    return seen


def main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(description="Smoke dev do TP2 (1 partição)")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="r-events-smoke")
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=1)
    args = parser.parse_args(argv)

    run_id = uuid.uuid4().hex[:8]
    admin = make_admin(args.bootstrap)

    created = create_topic(
        admin,
        args.topic,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
        min_insync_replicas=1,
    )
    verified = verify_topic(admin, args.topic, args.partitions)
    if not verified["ok"]:
        print(json.dumps(verified, indent=2, ensure_ascii=False))
        print(f"ERRO: {args.topic} não tem {args.partitions} partição(ões)", file=sys.stderr)
        return 1

    produced = produce_pair(args.topic, args.bootstrap, run_id)
    targets = {item["offset"] for item in produced}
    consumed = consume_until(args.topic, args.bootstrap, targets)

    by_offset = {item["offset"]: item for item in consumed}
    missing = sorted(targets - set(by_offset))
    mismatched = [
        offset for offset in sorted(targets & set(by_offset))
        if by_offset[offset]["value"] != next(
            item["value"] for item in produced if item["offset"] == offset
        )
    ]
    bad_partitions = [item for item in consumed if item["partition"] != 0]
    off_payloads = []
    for item in consumed:
        try:
            off_payloads.append(json.loads(item["value"]))
        except (TypeError, ValueError):
            off_payloads.append({})

    ok = (
        not missing
        and not mismatched
        and not bad_partitions
        and [item["offset"] for item in consumed] == sorted(
            item["offset"] for item in consumed
        )
    )

    summary = {
        "bootstrap": args.bootstrap,
        "topic": args.topic,
        "partitions_ok": verified["ok"],
        "topic_created": created.get("created"),
        "produced": produced,
        "consumed_count": len(consumed),
        "consumed_offsets": [item["offset"] for item in consumed],
        "missing_offsets": missing,
        "mismatched_offsets": mismatched,
        "non_partition_0": bad_partitions,
        "protocol_types": [p.get("type") for p in off_payloads],
        "ok": ok,
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
