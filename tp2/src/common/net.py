"""Utilitários TCP/JSONL: conexões, servidores de linhas e barreira de início."""

from __future__ import annotations

import json
import socket
import socketserver
import threading
import time
from typing import Any, Callable, Optional

from common.messages import ACK, CLIENT_READY, ERROR, READY, START


def _encode(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"


class JsonlConnection:
    """Conexão TCP com uma mensagem JSON por linha."""

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self.rfile = sock.makefile("rb")
        self.wfile = sock.makefile("wb")

    def send(self, payload: dict) -> None:
        self.wfile.write(_encode(payload))
        self.wfile.flush()

    def recv(self, timeout: Optional[float] = None) -> Optional[dict]:
        self.sock.settimeout(timeout)
        try:
            line = self.rfile.readline()
        except (socket.timeout, TimeoutError):
            raise TimeoutError("timeout aguardando mensagem JSONL") from None
        finally:
            self.sock.settimeout(None)
        if not line:
            return None
        return json.loads(line)

    def close(self) -> None:
        try:
            self.rfile.close()
            self.wfile.close()
            self.sock.close()
        except OSError:
            pass


def connect(host: str, port: int, timeout: float = 10.0) -> JsonlConnection:
    sock = socket.create_connection((host, port), timeout=timeout)
    sock.settimeout(None)
    return JsonlConnection(sock)


def wait_port(host: str, port: int, timeout: float = 20.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.05)
    return False


class _Handler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        for raw in self.rfile:
            raw = raw.strip()
            if not raw:
                continue
            try:
                message = json.loads(raw)
            except ValueError:
                self.send({"type": ERROR, "error": "JSON inválido"})
                continue
            try:
                self.server.dispatch(message, self)
            except Exception as exc:  # noqa: BLE001 - isolar falha de um cliente
                self.send({"type": ERROR, "error": str(exc)})

    def send(self, payload: dict) -> None:
        self.wfile.write(_encode(payload))
        self.wfile.flush()


Dispatch = Callable[[dict, _Handler], None]


class JsonlServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, host: str, port: int, dispatch: Dispatch) -> None:
        self.dispatch = dispatch
        super().__init__((host, port), _Handler)

    @property
    def port(self) -> int:
        return int(self.server_address[1])

    def serve_in_thread(self) -> threading.Thread:
        thread = threading.Thread(target=self.serve_forever, daemon=True)
        thread.start()
        return thread

    def stop(self) -> None:
        self.shutdown()
        self.server_close()


class StartBarrierServer(JsonlServer):
    """Barrera de inicio: cada cliente avisa CLIENT_READY y recibe START."""

    def __init__(
        self, host: str, port: int, expected: int, timeout: float = 120.0,
    ) -> None:
        if expected < 1:
            raise ValueError("expected deve ser >= 1")
        self.expected = expected
        self._barrier = threading.Barrier(expected, timeout=timeout)
        super().__init__(host, port, self._dispatch)

    def _dispatch(self, message: dict, handler: _Handler) -> None:
        if message.get("type") != CLIENT_READY:
            handler.send({"type": ERROR, "error": f"esperado {CLIENT_READY}"})
            return
        try:
            self._barrier.wait()
        except threading.BrokenBarrierError:
            handler.send({"type": ERROR, "error": "barreira quebrada"})
            return
        handler.send({"type": START})


class ReadyCollector(JsonlServer):
    """Coleta READY dos nós (canal de controle, fora do log Kafka)."""

    def __init__(self, host: str, port: int, expected: int) -> None:
        self.expected = expected
        self._lock = threading.Lock()
        self._ready: list[str] = []
        self._event = threading.Event()
        super().__init__(host, port, self._dispatch)

    def _dispatch(self, message: dict, handler: _Handler) -> None:
        if message.get("type") != READY:
            handler.send({"type": ERROR, "error": f"esperado {READY}"})
            return
        process_id = str(message.get("process_id"))
        with self._lock:
            self._ready.append(process_id)
            if len(self._ready) >= self.expected:
                self._event.set()
        handler.send({"type": ACK})

    def wait(self, timeout: float = 60.0) -> bool:
        return self._event.wait(timeout)

    @property
    def ready(self) -> tuple:
        with self._lock:
            return tuple(self._ready)
