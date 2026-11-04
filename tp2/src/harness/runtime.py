"""Utilitários de execução: subprocessos, portas, STATUS e despejo do log Kafka."""

from __future__ import annotations

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

from confluent_kafka import Consumer, KafkaError, TopicPartition

from common.net import connect, wait_port

TP2_ROOT = Path(__file__).resolve().parents[2]
SRC = TP2_ROOT / "src"
PYTHON = sys.executable
ENV = os.environ.copy()
ENV["PYTHONPATH"] = str(SRC) + os.pathsep + ENV.get("PYTHONPATH", "")


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
    stdout = (out_dir / f"{name}.out.log").open("w", encoding="utf-8")
    stderr = (out_dir / f"{name}.err.log").open("w", encoding="utf-8")
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


def stop_process(proc: subprocess.Popen, timeout: float = 15.0) -> int:
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


def wait_port_ready(host: str, port: int, timeout: float = 30.0) -> bool:
    return wait_port(host, port, timeout=timeout)


def wait_nodes_consumed(nodes: list[dict], expected: int, timeout: float = 60.0) -> bool:
    """Espera todos os nós reportarem STATUS.delivered >= expected."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        values = []
        for node in nodes:
            try:
                connection = connect("127.0.0.1", node["port"], timeout=2.0)
                connection.send({"type": "STATUS"})
                response = connection.recv(timeout=3.0)
                connection.close()
                values.append(int(response["delivered"]))
            except (OSError, TimeoutError, KeyError):
                break
        else:
            if len(values) == len(nodes) and all(value >= expected for value in values):
                return True
        time.sleep(0.1)
    return False


def dump_topic(
    bootstrap: str,
    topic: str,
    expected: int,
    path: Path,
    timeout: float = 60.0,
) -> list[dict]:
    """Lê o tópico inteiro desde o offset 0 e grava kafka_log.jsonl."""
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"tp2-dump-{uuid.uuid4().hex}",
        "client.id": "tp2-dump",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "enable.partition.eof": True,
    })
    consumer.assign([TopicPartition(topic, 0, 0)])
    records: list[dict] = []
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(0.2)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    break
                raise RuntimeError(str(message.error()))
            records.append({
                "offset": message.offset(),
                "partition": message.partition(),
                "timestamp_ms": message.timestamp()[1],
                "key": message.key().decode("utf-8") if message.key() else None,
                "value": message.value().decode("utf-8") if message.value() else None,
            })
            if expected and len(records) >= expected:
                break
    finally:
        consumer.close()

    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
    return records
