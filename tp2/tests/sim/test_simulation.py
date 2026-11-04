"""Simulação determinística: 5 nós, log falso ordenado, sem Kafka.

Cada nó mantém uma réplica da fila global e recebe o log em ordens/ritmos
diferentes (sempre prefixos na ordem de offsets). A simulação intercala
publicações, entregas, entradas e saídas da seção crítica de forma aleatória,
mantendo a seção crítica ocupada por alguns passos para expor sobreposição.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Optional

import pytest

from common.messages import ACQUIRE, RELEASE, ProtocolMessage
from sync_node.kafka_sync import (
    ENQUEUED,
    INVARIANT_VIOLATION,
    QueueState,
)

RESOURCE = "R"


@dataclass
class SimNode:
    index: int
    requests: int
    state: QueueState = field(default_factory=QueueState)
    cursor: int = 0
    waiting: Optional[str] = None
    inside: Optional[str] = None
    inside_steps: int = 0
    releasing: Optional[str] = None
    delivered: list = field(default_factory=list)
    pending: deque = field(init=False)

    def __post_init__(self) -> None:
        self.pending = deque(
            f"sync-{self.index}-{number:04d}" for number in range(1, self.requests + 1)
        )

    @property
    def process_id(self) -> str:
        return f"sync-{self.index}"


class Simulation:
    def __init__(self, seed: int, num_nodes: int = 5, requests: int = 6) -> None:
        import random

        self.rng = random.Random(seed)
        self.nodes = [SimNode(index, requests) for index in range(num_nodes)]
        self.log: list[tuple[int, ProtocolMessage]] = []
        self.acquire_offsets: dict[str, int] = {}
        self.active = 0
        self.max_active = 0
        self.completed = 0
        self.total = num_nodes * requests
        self.violations: list = []
        self.enter_order: list[tuple[int, str]] = []

    def _publish(self, message: ProtocolMessage) -> int:
        offset = len(self.log)
        self.log.append((offset, message))
        if message.type == ACQUIRE:
            self.acquire_offsets[message.request_id] = offset
        return offset

    def _deliver(self, node: SimNode) -> None:
        offset, message = self.log[node.cursor]
        action = node.state.apply(offset, message)
        node.delivered.append(offset)
        node.cursor += 1
        if action == INVARIANT_VIOLATION:
            self.violations.append((node.process_id, offset, message.request_id))

    def _step(self) -> None:
        # Encerra nós cujo RELEASE já foi entregue e removido da fila local.
        for node in self.nodes:
            if node.releasing is not None and not node.state.contains(node.releasing):
                node.releasing = None
                self.completed += 1

        if self.completed >= self.total:
            return

        actions: list[tuple[str, SimNode]] = []
        for node in self.nodes:
            if node.cursor < len(self.log):
                actions.append(("deliver", node))
            if node.inside is not None:
                actions.append(("exit", node))
            elif node.releasing is None and node.waiting is None and node.pending:
                actions.append(("acquire", node))
            if node.waiting is not None:
                head = node.state.head()
                if (
                    head is not None
                    and head.process_id == node.process_id
                    and head.request_id == node.waiting
                ):
                    actions.append(("enter", node))

        if not actions:
            raise AssertionError("simulação sem progresso")

        kind, node = self.rng.choice(actions)
        if kind == "deliver":
            self._deliver(node)
        elif kind == "acquire":
            request_id = node.pending.popleft()
            node.waiting = request_id
            self._publish(ProtocolMessage(ACQUIRE, RESOURCE, node.process_id, request_id))
        elif kind == "enter":
            assert self.active == 0, "exclusão mútua violada na simulação"
            self.active = 1
            self.max_active = max(self.max_active, self.active)
            node.inside = node.waiting
            node.waiting = None
            node.inside_steps = self.rng.randint(1, 4)
            self.enter_order.append((self.acquire_offsets[node.inside], node.inside))
        elif kind == "exit":
            assert node.inside_steps > 0
            node.inside_steps -= 1
            if node.inside_steps == 0:
                request_id = node.inside
                node.inside = None
                self.active = 0
                node.releasing = request_id
                self._publish(ProtocolMessage(RELEASE, RESOURCE, node.process_id, request_id))
        else:  # pragma: no cover - defesa
            raise AssertionError(f"ação desconhecida: {kind}")

    def run(self, max_steps: int = 2_000_000) -> "Simulation":
        steps = 0
        while self.completed < self.total:
            steps += 1
            if steps > max_steps:
                raise AssertionError(f"simulação não terminou em {max_steps} passos")
            self._step()
        self._drain()
        return self

    def _drain(self) -> None:
        """Consome o restante do log em todos os nós (comprova V5)."""
        while True:
            lagging = [node for node in self.nodes if node.cursor < len(self.log)]
            if not lagging:
                return
            self._deliver(self.rng.choice(lagging))

    def assert_properties(self) -> None:
        assert not self.violations, f"violações: {self.violations[:5]}"
        assert self.max_active == 1, f"max_active={self.max_active}"
        assert self.active == 0
        assert self.completed == self.total
        assert len(self.enter_order) == self.total

        offsets = [offset for offset, _ in self.enter_order]
        assert offsets == sorted(offsets), "entradas fora da ordem do log"
        assert len(set(offsets)) == len(offsets), "offset de ACQUIRE repetido"

        expected_deliveries = list(range(len(self.log)))
        for node in self.nodes:
            assert node.cursor == len(self.log)
            assert node.delivered == expected_deliveries
            assert node.state.entries() == (), f"{node.process_id} terminou com fila não vazia"
            assert node.state.violations == ()


@pytest.mark.parametrize("num_nodes,requests", [(5, 6), (5, 10), (3, 4)])
def test_protocol_properties_many_seeds(num_nodes: int, requests: int) -> None:
    for seed in range(1000):
        simulation = Simulation(seed, num_nodes=num_nodes, requests=requests).run()
        try:
            simulation.assert_properties()
        except AssertionError as exc:  # dá contexto da semente
            raise AssertionError(f"seed={seed}: {exc}") from exc


def test_all_nodes_receive_identical_log() -> None:
    simulation = Simulation(seed=42, num_nodes=5, requests=4).run()
    sequences = {tuple(node.delivered) for node in simulation.nodes}
    assert len(sequences) == 1
    assert sequences.pop() == tuple(range(len(simulation.log)))


def test_every_acquire_has_exactly_one_release() -> None:
    simulation = Simulation(seed=7, num_nodes=5, requests=5).run()
    acquires = [msg.request_id for _, msg in simulation.log if msg.type == ACQUIRE]
    releases = [msg.request_id for _, msg in simulation.log if msg.type == RELEASE]
    assert sorted(acquires) == sorted(releases)
    assert len(acquires) == simulation.total
