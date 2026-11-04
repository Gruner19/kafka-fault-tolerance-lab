#!/usr/bin/env python3
"""Observador de instrumentação (fora do Kafka e fora do protocolo).

Recebe BEGIN/END dos nós, mantém um contador atômico de acessos ativos e
registra OVERLAP quando a capacidade é excedida. Não concede permissão, não
bloqueia BEGIN alheio e não é um mutex.
"""

from __future__ import annotations

import argparse
import signal
import threading
import time
from typing import Optional

from common.jsonl_log import JsonlLogger
from common.messages import ACK, BEGIN, END, ERROR, OVERLAP, SUMMARY
from common.net import JsonlConnection, JsonlServer, connect


class ObserverClient:
    """Cliente usado pelo nó; cada chamada espera o ACK do observador."""

    def __init__(self, host: str, port: int, timeout: float = 10.0) -> None:
        self._conn: JsonlConnection = connect(host, port, timeout=timeout)

    def _call(self, payload: dict) -> dict:
        self._conn.send(payload)
        response = self._conn.recv(timeout=60.0)
        if not response or response.get("type") != ACK:
            raise RuntimeError(f"observador sem ACK: {response}")
        return response

    def begin(self, process_id: str, request_id: str) -> dict:
        return self._call({"type": BEGIN, "process_id": process_id, "request_id": request_id})

    def end(self, process_id: str, request_id: str) -> dict:
        return self._call({"type": END, "process_id": process_id, "request_id": request_id})

    def close(self) -> None:
        self._conn.close()


class ObserverState:
    def __init__(self, capacity: int, logger: JsonlLogger) -> None:
        self.capacity = capacity
        self._logger = logger
        self._lock = threading.Lock()
        self._active = 0
        self._max_active = 0
        self._seq = 0
        self._active_ids: dict[str, str] = {}
        self._begins = 0
        self._ends = 0
        self._overlaps: list[dict] = []

    def begin(self, process_id: str, request_id: str) -> int:
        with self._lock:
            self._seq += 1
            self._begins += 1
            self._active += 1
            self._max_active = max(self._max_active, self._active)
            self._active_ids[request_id] = process_id
            if self._active > self.capacity:
                overlap = {
                    "obs_seq": self._seq,
                    "active": self._active,
                    "capacity": self.capacity,
                    "request_ids": sorted(self._active_ids),
                }
                self._overlaps.append(overlap)
                self._logger.log(OVERLAP, **overlap)
            self._logger.log(
                BEGIN,
                obs_seq=self._seq,
                process_id=process_id,
                request_id=request_id,
                active=self._active,
                max_active=self._max_active,
            )
            return self._seq

    def end(self, process_id: str, request_id: str) -> int:
        with self._lock:
            self._seq += 1
            self._ends += 1
            orphan = request_id not in self._active_ids
            if not orphan:
                del self._active_ids[request_id]
            self._active = max(0, self._active - 1)
            self._logger.log(
                END,
                obs_seq=self._seq,
                process_id=process_id,
                request_id=request_id,
                active=self._active,
                orphan=orphan,
            )
            return self._seq

    def summary(self) -> dict:
        with self._lock:
            return {
                "max_active": self._max_active,
                "capacity": self.capacity,
                "overlaps": len(self._overlaps),
                "begins": self._begins,
                "ends": self._ends,
            }


class ObserverServer(JsonlServer):
    def __init__(self, host: str, port: int, state: ObserverState) -> None:
        self.state = state
        super().__init__(host, port, self._dispatch)

    def _dispatch(self, message: dict, handler) -> None:
        kind = message.get("type")
        process_id = str(message.get("process_id", ""))
        request_id = str(message.get("request_id", ""))
        if not process_id or not request_id:
            handler.send({"type": ERROR, "error": "BEGIN/END exige process_id e request_id"})
            return
        if kind == BEGIN:
            seq = self.state.begin(process_id, request_id)
        elif kind == END:
            seq = self.state.end(process_id, request_id)
        else:
            handler.send({"type": ERROR, "error": f"tipo inválido: {kind}"})
            return
        handler.send({"type": ACK, "obs_seq": seq})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Observador de instrumentação do TP2")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7100)
    parser.add_argument("--capacity", type=int, default=1)
    parser.add_argument("--run-id", default="local")
    parser.add_argument("--log-dir", required=True)
    return parser


def main(argv: Optional[list] = None) -> int:
    args = build_parser().parse_args(argv)
    logger = JsonlLogger(f"{args.log_dir}/observer.jsonl", actor="observer", run_id=args.run_id)
    state = ObserverState(args.capacity, logger)
    server = ObserverServer(args.host, args.port, state)
    stop = threading.Event()

    def on_signal(signum, frame) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    server.serve_in_thread()
    print(f"observer listening on {args.host}:{server.port}", flush=True)
    try:
        while not stop.wait(0.2):
            pass
    finally:
        server.stop()
        summary = state.summary()
        logger.log(SUMMARY, **summary)
        logger.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
