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

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

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

    def __init__(self, capacity: int = 1) -> None:
        if capacity < 1:
            raise ValueError("capacity deve ser >= 1")
        self.capacity = capacity
        self._cond = threading.Condition()
        self._queue: list[QueueEntry] = []
        self._seen: set[str] = set()
        self._released: set[str] = set()
        self._violations: list[dict] = []
        self._aborted = False

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
    def wait_until_position(
        self,
        process_id: str,
        request_id: str,
        limit: int = 1,
        timeout: Optional[float] = None,
    ) -> bool:
        """Bloqueia até que (process_id, request_id) esteja entre os `limit` primeiros.

        `limit=1` implementa o mutex do B6 (só a cabeça entra). `limit=K>1`
        implementa a extensão semáforo: até K pedidos podem estar em CS.

        O timeout existe apenas para testes/encerramento; o protocolo não usa
        leases nem timeouts para autorizar (seção 2.4).
        """
        if limit < 1:
            raise ValueError("limit deve ser >= 1")
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while True:
                if self._aborted:
                    return False
                for index, entry in enumerate(self._queue):
                    if entry.process_id == process_id and entry.request_id == request_id:
                        if index < limit:
                            return True
                        break
                if deadline is None:
                    self._cond.wait()
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(remaining)

    def wait_until_head(
        self, process_id: str, request_id: str, timeout: Optional[float] = None,
    ) -> bool:
        return self.wait_until_position(
            process_id, request_id, limit=1, timeout=timeout,
        )

    def abort(self) -> None:
        """Desbloqueia esperas no encerramento do processo (não é parte do protocolo)."""
        with self._cond:
            self._aborted = True
            self._cond.notify_all()


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
        capacity: int = 1,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity deve ser >= 1")
        self.process_id = process_id
        self.resource = resource
        self.publish = publish
        self.state = state if state is not None else QueueState()
        self._log = log
        self.timeout = timeout
        self.capacity = capacity

    def _emit(self, event: str, **fields: object) -> None:
        if self._log is not None:
            self._log(event, **fields)

    def acquire(self, request_id: str) -> int:
        message = ProtocolMessage(ACQUIRE, self.resource, self.process_id, request_id)
        offset = self.publish(message)
        # Só registra a aquisição depois que a própria mensagem foi entregue e o
        # pedido virou cabeça: assim `queue_head` é sempre o pedido autorizado.
        if not self.state.wait_until_position(
            self.process_id, request_id, limit=self.capacity, timeout=self.timeout,
        ):
            raise TimeoutError(f"ACQUIRE {request_id} não chegou à posição de entrada")
        snapshot = self.state.snapshot()
        self._emit(
            ACQUIRE,
            request_id=request_id,
            process_id=self.process_id,
            offset=offset,
            queue_head=snapshot["queue_head"],
            queue_len=snapshot["queue_len"],
        )
        return offset

    def release(self, request_id: str) -> int:
        message = ProtocolMessage(RELEASE, self.resource, self.process_id, request_id)
        # A foto é tirada antes de publicar: o consumidor local pode remover o
        # pedido da fila antes de o worker voltar do delivery report. No estado
        # pré-publicação o pedido é, por invariante, a cabeça da fila.
        snapshot = self.state.snapshot()
        offset = self.publish(message)
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


def verify_single_partition(bootstrap: str, topic: str, timeout: float = 10.0) -> int:
    """Aborta se o tópico não existir ou não tiver exatamente 1 partição."""
    admin = AdminClient({"bootstrap.servers": bootstrap, "client.id": "tp2-verify"})
    metadata = admin.list_topics(timeout=timeout).topics.get(topic)
    if metadata is None or metadata.error is not None:
        detail = "sem metadata" if metadata is None else str(metadata.error)
        raise RuntimeError(f"tópico {topic!r} indisponível ({detail})")
    count = len(metadata.partitions)
    if count != 1:
        raise RuntimeError(
            f"tópico {topic!r} tem {count} partições; o B6 exige exatamente 1"
        )
    return 0


class KafkaLog:
    """Adaptador Kafka: produtor síncrono (partição 0) e consumidor dedicado."""

    def __init__(
        self,
        bootstrap: str,
        topic: str,
        group_id: str,
        process_id: str,
        delivery_timeout: float = 10.0,
    ) -> None:
        self.topic = topic
        self._tp = TopicPartition(topic, 0)
        self._delivery_timeout = delivery_timeout
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "client.id": f"{process_id}-producer",
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 0,
            "max.in.flight.requests.per.connection": 5,
            "message.timeout.ms": 10000,
            "retries": 10,
        })
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap,
            "client.id": f"{process_id}-consumer",
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "fetch.wait.max.ms": 10,
            "fetch.min.bytes": 1,
            "session.timeout.ms": 10000,
            "max.poll.interval.ms": 300000,
        })

    def subscribe_and_wait(self, timeout: float = 30.0) -> int:
        """Assina o tópico e espera a atribuição da partição 0 (readiness).

        Devolve a posição inicial da partição (0 quando o tópico está vazio),
        registrando assim uma posição conhecida antes da carga.
        """
        self.consumer.subscribe([self.topic])
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.consumer.poll(0.1)
            assignment = self.consumer.assignment()
            if assignment:
                if len(assignment) != 1 or assignment[0].partition != 0:
                    raise RuntimeError(f"atribuição inesperada: {assignment}")
                positions = self.consumer.position([self._tp])
                if positions and positions[0].offset >= 0:
                    return int(positions[0].offset)
                # Com o tópico vazio o position() pode continuar INVALID até o
                # primeiro fetch; o low watermark é a posição inicial efetiva.
                low, _ = self.consumer.get_watermark_offsets(self._tp, timeout=2.0)
                return int(low)
        raise TimeoutError(f"consumidor não recebeu a partição 0 de {self.topic!r}")

    def publish(self, message: ProtocolMessage) -> int:
        outcome: dict = {}
        done = threading.Event()

        def on_delivery(error, delivered) -> None:
            if error is not None:
                outcome["error"] = error
            else:
                outcome["offset"] = delivered.offset()
                outcome["partition"] = delivered.partition()
            done.set()

        self.producer.produce(
            self.topic,
            key=message.resource.encode("utf-8"),
            value=message.to_bytes(),
            partition=0,
            on_delivery=on_delivery,
        )
        deadline = time.monotonic() + self._delivery_timeout
        while not done.is_set():
            self.producer.poll(0.01)
            if time.monotonic() >= deadline:
                raise TimeoutError(f"delivery report não chegou para {message.request_id}")
        if "error" in outcome:
            raise RuntimeError(str(outcome["error"]))
        return int(outcome["offset"])

    def poll(self, timeout: float = 0.05):
        record = self.consumer.poll(timeout)
        if record is None:
            return None
        if record.error():
            if record.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise RuntimeError(str(record.error()))
        return record.partition(), record.offset(), record.value()

    def close(self) -> None:
        try:
            self.producer.flush(5.0)
        finally:
            self.consumer.close()
