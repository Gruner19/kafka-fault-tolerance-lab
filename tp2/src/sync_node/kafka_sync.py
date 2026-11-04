"""Núcleo do B6: fila replicada sobre log totalmente ordenado.

A regra de acesso é implementada aqui, sem depender de Kafka. Cada processo
mantém uma réplica da fila global de pedidos pendentes; o log (uma única
partição) define a ordem total. O broker apenas transporta e ordena.
"""

from __future__ import annotations

import threading
import time
from dataclasses import asdict, dataclass
from typing import Callable, Optional

from common.messages import ACQUIRE, RELEASE, ProtocolMessage

ENQUEUED = "ENQUEUED"
DUPLICATE_IGNORED = "DUPLICATE_IGNORED"
RELEASED = "RELEASED"
INVARIANT_VIOLATION = "INVARIANT_VIOLATION"


@dataclass(frozen=True)
class QueueEntry:
    offset: int
    process_id: str
    request_id: str

    def to_dict(self) -> dict:
        return asdict(self)


class QueueState:
    """Réplica local da fila global.

    `Condition` protege apenas as estruturas internas (permitido pela spec); não
    é usada como mecanismo de coordenação entre nós.
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._queue: list[QueueEntry] = []
        self._seen: set[str] = set()
        self._released: set[str] = set()
        self._violations: list[dict] = []

    # -- consulta ---------------------------------------------------------
    def head(self) -> Optional[QueueEntry]:
        with self._cond:
            return self._queue[0] if self._queue else None

    def entries(self) -> tuple[QueueEntry, ...]:
        with self._cond:
            return tuple(self._queue)

    def contains(self, request_id: str) -> bool:
        with self._cond:
            return any(entry.request_id == request_id for entry in self._queue)

    @property
    def violations(self) -> tuple[dict, ...]:
        with self._cond:
            return tuple(self._violations)

    def snapshot(self) -> dict:
        with self._cond:
            head = self._queue[0] if self._queue else None
            return {
                "queue_len": len(self._queue),
                "queue_head": head.request_id if head else None,
                "queue": [entry.to_dict() for entry in self._queue],
            }

    # -- transições -------------------------------------------------------
    def apply_acquire(self, offset: int, process_id: str, request_id: str) -> str:
        with self._cond:
            if request_id in self._seen:
                return DUPLICATE_IGNORED
            self._seen.add(request_id)
            self._queue.append(QueueEntry(offset, process_id, request_id))
            self._cond.notify_all()
            return ENQUEUED

    def apply_release(self, offset: int, process_id: str, request_id: str) -> str:
        with self._cond:
            # Duplicata de retry: o pedido já saiu da fila legitimamente.
            if request_id in self._released:
                return DUPLICATE_IGNORED

            head = self._queue[0] if self._queue else None
            if head is None or head.process_id != process_id or head.request_id != request_id:
                violation = {
                    "offset": offset,
                    "process_id": process_id,
                    "request_id": request_id,
                    "queue_head": head.request_id if head else None,
                    "queue_len": len(self._queue),
                }
                self._violations.append(violation)
                # Recuperação defensiva indicada pela spec: remover por request_id.
                self._queue = [
                    entry for entry in self._queue if entry.request_id != request_id
                ]
                self._cond.notify_all()
                return INVARIANT_VIOLATION

            self._queue.pop(0)
            self._released.add(request_id)
            self._cond.notify_all()
            return RELEASED

    def apply(self, offset: int, message: ProtocolMessage) -> str:
        if message.type == ACQUIRE:
            return self.apply_acquire(offset, message.process_id, message.request_id)
        if message.type == RELEASE:
            return self.apply_release(offset, message.process_id, message.request_id)
        raise ValueError(f"tipo de mensagem inválido: {message.type!r}")

    # -- espera -----------------------------------------------------------
    def wait_until_head(
        self, process_id: str, request_id: str, timeout: Optional[float] = None,
    ) -> bool:
        """Bloqueia até que (process_id, request_id) seja a cabeça da fila.

        O timeout existe apenas para testes/encerramento; o protocolo não usa
        leases nem timeouts para autorizar (seção 2.4).
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while True:
                head = self._queue[0] if self._queue else None
                if head is not None and head.process_id == process_id and head.request_id == request_id:
                    return True
                if deadline is None:
                    self._cond.wait()
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(remaining)


PublishFn = Callable[[ProtocolMessage], int]
LogFn = Callable[..., object]


class SyncProtocol:
    """API acquire/release separada da seção crítica simulada.

    O `publish` injetado deve ser síncrono e devolver o offset do registro
    (delivery report do produtor Kafka). Em testes, pode ser uma lista.
    """

    def __init__(
        self,
        process_id: str,
        publish: PublishFn,
        resource: str = "R",
        state: Optional[QueueState] = None,
        log: Optional[LogFn] = None,
        timeout: Optional[float] = None,
    ) -> None:
        self.process_id = process_id
        self.resource = resource
        self.publish = publish
        self.state = state if state is not None else QueueState()
        self._log = log
        self.timeout = timeout

    def _emit(self, event: str, **fields: object) -> None:
        if self._log is not None:
            self._log(event, **fields)

    def acquire(self, request_id: str) -> int:
        message = ProtocolMessage(ACQUIRE, self.resource, self.process_id, request_id)
        offset = self.publish(message)
        snapshot = self.state.snapshot()
        self._emit(
            ACQUIRE,
            request_id=request_id,
            process_id=self.process_id,
            offset=offset,
            queue_head=snapshot["queue_head"],
            queue_len=snapshot["queue_len"],
        )
        if not self.state.wait_until_head(self.process_id, request_id, timeout=self.timeout):
            raise TimeoutError(f"ACQUIRE {request_id} não chegou à cabeça da fila")
        return offset

    def release(self, request_id: str) -> int:
        message = ProtocolMessage(RELEASE, self.resource, self.process_id, request_id)
        offset = self.publish(message)
        snapshot = self.state.snapshot()
        self._emit(
            RELEASE,
            request_id=request_id,
            process_id=self.process_id,
            offset=offset,
            queue_head=snapshot["queue_head"],
            queue_len=snapshot["queue_len"],
        )
        return offset

    def on_deliver(self, offset: int, payload: str | bytes | ProtocolMessage) -> str:
        message = payload if isinstance(payload, ProtocolMessage) else ProtocolMessage.from_json(payload)
        action = self.state.apply(offset, message)
        if action != ENQUEUED:
            snapshot = self.state.snapshot()
            self._emit(
                action,
                request_id=message.request_id,
                process_id=message.process_id,
                message_type=message.type,
                offset=offset,
                queue_head=snapshot["queue_head"],
                queue_len=snapshot["queue_len"],
            )
        return action
