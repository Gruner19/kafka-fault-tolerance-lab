"""Servidor TCP/JSONL de um nó: recebe REQUEST dos clientes e expõe STATUS."""

from __future__ import annotations

from concurrent.futures import Future
from typing import Callable

from common.messages import COMMITTED, ERROR, REQUEST, STATUS
from common.net import JsonlServer

SubmitFn = Callable[[dict], Future]
StatusFn = Callable[[], dict]


class NodeClientServer(JsonlServer):
    def __init__(
        self, host: str, port: int, submit: SubmitFn, status: StatusFn,
    ) -> None:
        self._submit = submit
        self._status = status
        super().__init__(host, port, self._dispatch)

    def _dispatch(self, message: dict, handler) -> None:
        kind = message.get("type")
        if kind == REQUEST:
            request_id = str(message.get("request_id", ""))
            if not request_id:
                handler.send({"type": ERROR, "error": "REQUEST sem request_id"})
                return
            future = self._submit(message)
            try:
                future.result()
            except Exception as exc:  # noqa: BLE001 - devolver erro ao cliente
                handler.send({
                    "type": ERROR, "request_id": request_id, "error": str(exc),
                })
                return
            handler.send({"type": COMMITTED, "request_id": request_id})
            return

        if kind == STATUS:
            handler.send({"type": STATUS, **self._status()})
            return

        handler.send({"type": ERROR, "error": f"tipo desconhecido: {kind}"})
