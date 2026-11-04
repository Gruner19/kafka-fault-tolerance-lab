#!/usr/bin/env python3
"""Processo do Cluster Sync (B6): fila replicada + seção crítica simulada."""

from __future__ import annotations

import argparse
import hashlib
import queue
import signal
import sys
import threading
import time
from concurrent.futures import Future
from pathlib import Path
from typing import Optional

from common.config import rng_for
from common.jsonl_log import JsonlLogger
from common.messages import (
    ACK,
    COMMITTED,
    DELIVERED_DIGEST,
    ENTER,
    ERROR,
    EXIT,
    INVARIANT_VIOLATION,
    READY,
    RELEASE,
    REQUEST,
)
from common.net import connect
from observer.observer import ObserverClient
from sync_node.client_server import NodeClientServer
from sync_node.critical_section import SimulatedCriticalSection
from sync_node.kafka_sync import (
    ENQUEUED,
    KafkaLog,
    QueueState,
    SyncProtocol,
    verify_single_partition,
)

RESOURCE = "R"


class SyncNode:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.process_id = args.process_id
        self.run_id = args.run_id
        self.sync_enabled = args.sync_enabled
        self.rng = rng_for(args.seed, args.process_id)
        self.logger = JsonlLogger(
            Path(args.log_dir) / f"node-{args.process_id}.jsonl",
            actor=args.process_id,
            run_id=args.run_id,
        )

        self.stop_event = threading.Event()
        self.requests: queue.Queue = queue.Queue()
        self.state = QueueState()
        self.protocol: Optional[SyncProtocol] = None
        self.kafka: Optional[KafkaLog] = None
        self.observer: Optional[ObserverClient] = None
        self.server: Optional[NodeClientServer] = None
        self.consumer_thread: Optional[threading.Thread] = None
        self.worker_thread: Optional[threading.Thread] = None

        self._counter_lock = threading.Lock()
        self._delivered = 0
        self._last_offset = -1
        self._violations = 0
        self._digest = hashlib.sha256()
        self._critical_section = SimulatedCriticalSection()

    # -- integração com o servidor de clientes ---------------------------
    def submit(self, message: dict) -> Future:
        future: Future = Future()
        request_id = str(message["request_id"])
        self.logger.log(
            REQUEST,
            process_id=self.process_id,
            client_id=str(message.get("client_id", "")),
            request_id=request_id,
        )
        self.requests.put((message, future))
        return future

    def status(self) -> dict:
        snapshot = self.state.snapshot()
        with self._counter_lock:
            return {
                "process_id": self.process_id,
                "delivered": self._delivered,
                "last_offset": self._last_offset,
                "violations": self._violations,
                "queue_len": snapshot["queue_len"],
                "queue_head": snapshot["queue_head"],
                "sync_enabled": self.sync_enabled,
            }

    # -- threads ----------------------------------------------------------
    def _consume_loop(self) -> None:
        assert self.kafka is not None
        while not self.stop_event.is_set():
            try:
                record = self.kafka.poll(0.05)
            except Exception as exc:  # noqa: BLE001 - registrar e encerrar
                self.logger.log(ERROR, stage="consume", error=str(exc))
                return
            if record is None:
                continue

            _, offset, value = record
            action = ENQUEUED
            if self.protocol is not None:
                try:
                    action = self.protocol.on_deliver(offset, value)
                except Exception as exc:  # noqa: BLE001 - payload inválido
                    self.logger.log(ERROR, stage="deliver", offset=offset, error=str(exc))
                    continue

            with self._counter_lock:
                self._delivered += 1
                self._last_offset = offset
                if action == INVARIANT_VIOLATION:
                    self._violations += 1
                self._digest.update(f"{offset},".encode("ascii"))

    def _worker_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                item = self.requests.get(timeout=0.1)
            except queue.Empty:
                continue
            if item is None:
                break

            message, future = item
            request_id = str(message["request_id"])
            client_id = str(message.get("client_id", ""))
            offset: Optional[int] = None
            try:
                if self.protocol is not None:
                    offset = self.protocol.acquire(request_id)

                snapshot = self.state.snapshot()
                self.logger.log(
                    ENTER,
                    process_id=self.process_id,
                    request_id=request_id,
                    offset=offset,
                    queue_head=snapshot["queue_head"],
                    queue_len=snapshot["queue_len"],
                )

                assert self.observer is not None
                self.observer.begin(self.process_id, request_id)
                duration = self.rng.uniform(self.args.cs_min, self.args.cs_max)
                self._critical_section.execute(duration)
                self.observer.end(self.process_id, request_id)

                snapshot = self.state.snapshot()
                self.logger.log(
                    EXIT,
                    process_id=self.process_id,
                    request_id=request_id,
                    offset=offset,
                    queue_head=snapshot["queue_head"],
                    queue_len=snapshot["queue_len"],
                )

                if self.protocol is not None:
                    self.protocol.release(request_id)

                self.logger.log(
                    COMMITTED,
                    process_id=self.process_id,
                    client_id=client_id,
                    request_id=request_id,
                )
                future.set_result(None)
            except Exception as exc:  # noqa: BLE001 - propagar ao cliente
                self.logger.log(
                    ERROR, process_id=self.process_id, request_id=request_id, error=str(exc),
                )
                future.set_exception(exc)

    # -- ciclo de vida ----------------------------------------------------
    def _send_ready(self, timeout: float = 30.0) -> None:
        deadline = time.monotonic() + timeout
        last_error: Optional[Exception] = None
        while time.monotonic() < deadline:
            try:
                connection = connect(self.args.control_host, self.args.control_port, timeout=2.0)
                connection.send({"type": READY, "process_id": self.process_id})
                response = connection.recv(timeout=5.0)
                connection.close()
                if response is not None and response.get("type") == ACK:
                    return
                last_error = RuntimeError(f"READY sem ACK: {response}")
            except OSError as exc:
                last_error = exc
                time.sleep(0.1)
        raise TimeoutError(f"não foi possível avisar READY: {last_error}")

    def run(self) -> None:
        verify_single_partition(self.args.bootstrap, self.args.topic)

        self.kafka = KafkaLog(
            bootstrap=self.args.bootstrap,
            topic=self.args.topic,
            group_id=f"{self.process_id}-{self.run_id}",
            process_id=self.process_id,
        )
        start_offset = self.kafka.subscribe_and_wait(timeout=30.0)
        if self.sync_enabled:
            self.protocol = SyncProtocol(
                self.process_id,
                publish=self.kafka.publish,
                resource=RESOURCE,
                state=self.state,
                log=self.logger.log,
            )

        self.observer = ObserverClient(self.args.observer_host, self.args.observer_port)
        self.server = NodeClientServer(
            self.args.host, self.args.port, self.submit, self.status,
        )
        self.server.serve_in_thread()

        self.consumer_thread = threading.Thread(
            target=self._consume_loop, name=f"{self.process_id}-consumer", daemon=True,
        )
        self.consumer_thread.start()
        self.worker_thread = threading.Thread(
            target=self._worker_loop, name=f"{self.process_id}-worker", daemon=True,
        )
        self.worker_thread.start()

        self.logger.log(
            READY, process_id=self.process_id, partition=0, start_offset=start_offset,
        )
        self._send_ready()

        while not self.stop_event.wait(0.2):
            pass
        self.shutdown()

    def shutdown(self) -> None:
        self.state.abort()
        if self.server is not None:
            self.server.stop()
        if self.worker_thread is not None:
            self.worker_thread.join(timeout=5.0)
        if self.consumer_thread is not None:
            self.consumer_thread.join(timeout=5.0)
        if self.kafka is not None:
            self.kafka.close()
        if self.observer is not None:
            self.observer.close()

        with self._counter_lock:
            delivered = self._delivered
            last_offset = self._last_offset
            violations = self._violations
            digest = self._digest.hexdigest()
        self.logger.log(
            DELIVERED_DIGEST,
            process_id=self.process_id,
            delivered=delivered,
            last_offset=last_offset,
            violations=violations,
            sha256=digest,
        )
        self.logger.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Nó do Cluster Sync (B6 sobre Kafka)")
    parser.add_argument("--process-id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--log-dir", required=True)
    parser.add_argument("--control-host", default="127.0.0.1")
    parser.add_argument("--control-port", type=int, required=True)
    parser.add_argument("--observer-host", default="127.0.0.1")
    parser.add_argument("--observer-port", type=int, required=True)
    parser.add_argument("--sync-enabled", type=lambda v: v.lower() in ("1", "true", "yes", "y"), default=True)
    parser.add_argument("--cs-min", type=float, default=0.2)
    parser.add_argument("--cs-max", type=float, default=1.0)
    parser.add_argument("--seed", type=int, default=0)
    return parser


def main(argv: Optional[list] = None) -> int:
    args = build_parser().parse_args(argv)
    node = SyncNode(args)

    def on_signal(signum, frame) -> None:
        node.stop_event.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    try:
        node.run()
    except Exception as exc:  # noqa: BLE001 - falha de inicialização
        print(f"ERRO no nó {args.process_id}: {exc}", file=sys.stderr)
        node.logger.log(ERROR, process_id=args.process_id, error=str(exc))
        node.logger.close()
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
