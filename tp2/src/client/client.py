#!/usr/bin/env python3
"""Cliente de carga do Cluster Sync: um pedido por vez, por conexão persistente."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Optional

from common.config import rng_for
from common.jsonl_log import JsonlLogger
from common.messages import (
    CLIENT_READY,
    COMMITTED,
    ERROR,
    REQUEST,
    START,
)
from common.net import JsonlConnection, connect


class LoadClient:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.client_id = args.client_id
        self.rng = rng_for(args.seed, args.client_id)
        self.logger = JsonlLogger(
            Path(args.log_dir) / f"client-{args.client_id}.jsonl",
            actor=args.client_id,
            run_id=args.run_id,
        )
        self.connection: Optional[JsonlConnection] = None

    def _await_start(self) -> None:
        if not self.args.barrier_port:
            return
        barrier = connect(self.args.barrier_host, self.args.barrier_port, timeout=10.0)
        try:
            barrier.send({"type": CLIENT_READY, "client_id": self.client_id})
            response = barrier.recv(timeout=self.args.barrier_timeout)
        finally:
            barrier.close()
        if response is None or response.get("type") != START:
            raise RuntimeError(f"barreira não liberou o início: {response}")

    def run(self) -> int:
        self.connection = connect(self.args.node_host, self.args.node_port, timeout=10.0)
        try:
            self._await_start()
            for sequence in range(1, self.args.requests + 1):
                request_id = f"{self.client_id}-{sequence:04d}"
                self.logger.log(
                    REQUEST, client_id=self.client_id, request_id=request_id,
                )
                self.connection.send({
                    "type": REQUEST,
                    "client_id": self.client_id,
                    "request_id": request_id,
                })
                response = self.connection.recv(timeout=self.args.response_timeout)
                if response is None:
                    raise RuntimeError("nó encerrou a conexão")
                if response.get("type") == ERROR:
                    raise RuntimeError(
                        f"nó devolveu erro para {request_id}: {response.get('error')}"
                    )
                if response.get("type") != COMMITTED or response.get("request_id") != request_id:
                    raise RuntimeError(f"resposta inesperada: {response}")
                self.logger.log(
                    COMMITTED, client_id=self.client_id, request_id=request_id,
                )
                if sequence < self.args.requests:
                    time.sleep(self.rng.uniform(self.args.think_min, self.args.think_max))
        finally:
            self.connection.close()
            self.logger.close()
        return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cliente de carga do TP2")
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--node-host", default="127.0.0.1")
    parser.add_argument("--node-port", type=int, required=True)
    parser.add_argument("--requests", type=int, required=True)
    parser.add_argument("--think-min", type=float, default=1.0)
    parser.add_argument("--think-max", type=float, default=5.0)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--log-dir", required=True)
    parser.add_argument("--barrier-host", default="127.0.0.1")
    parser.add_argument("--barrier-port", type=int, default=0)
    parser.add_argument("--barrier-timeout", type=float, default=120.0)
    parser.add_argument("--response-timeout", type=float, default=120.0)
    return parser


def main(argv: Optional[list] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return LoadClient(args).run()
    except Exception as exc:  # noqa: BLE001 - cliente termina com erro visível
        print(f"ERRO no cliente {args.client_id}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
