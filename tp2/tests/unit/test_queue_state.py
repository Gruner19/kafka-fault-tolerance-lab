"""Testes unitários da máquina de estados da fila."""

from __future__ import annotations

import threading
import time

import pytest

from common.messages import ACQUIRE, RELEASE, ProtocolMessage
from sync_node.kafka_sync import (
    DUPLICATE_IGNORED,
    ENQUEUED,
    INVARIANT_VIOLATION,
    RELEASED,
    QueueState,
    SyncProtocol,
)


def test_acquire_enqueues_in_offset_order() -> None:
    state = QueueState()
    assert state.apply_acquire(10, "sync-1", "r1") == ENQUEUED
    assert state.apply_acquire(11, "sync-2", "r2") == ENQUEUED
    assert [entry.request_id for entry in state.entries()] == ["r1", "r2"]
    assert state.head().request_id == "r1"
    assert state.snapshot()["queue_len"] == 2


def test_duplicate_acquire_is_ignored() -> None:
    state = QueueState()
    assert state.apply_acquire(0, "sync-1", "r1") == ENQUEUED
    assert state.apply_acquire(1, "sync-1", "r1") == DUPLICATE_IGNORED
    assert len(state.entries()) == 1


def test_release_head_pops_and_marks_released() -> None:
    state = QueueState()
    state.apply_acquire(0, "sync-1", "r1")
    state.apply_acquire(1, "sync-2", "r2")
    assert state.apply_release(2, "sync-1", "r1") == RELEASED
    assert [entry.request_id for entry in state.entries()] == ["r2"]


def test_release_on_empty_queue_is_violation() -> None:
    state = QueueState()
    assert state.apply_release(0, "sync-1", "r1") == INVARIANT_VIOLATION
    assert len(state.violations) == 1
    assert state.violations[0]["queue_head"] is None


def test_release_out_of_order_is_violation_and_removes_entry() -> None:
    state = QueueState()
    state.apply_acquire(0, "sync-1", "r1")
    state.apply_acquire(1, "sync-2", "r2")
    state.apply_acquire(2, "sync-3", "r3")

    assert state.apply_release(3, "sync-2", "r2") == INVARIANT_VIOLATION
    assert [entry.request_id for entry in state.entries()] == ["r1", "r3"]
    assert state.violations[0]["queue_head"] == "r1"


def test_release_duplicate_after_success_is_ignored() -> None:
    """Reentrega de um RELEASE já aplicado não pode gerar falso positivo."""
    state = QueueState()
    state.apply_acquire(0, "sync-1", "r1")
    assert state.apply_release(1, "sync-1", "r1") == RELEASED
    assert state.apply_release(1, "sync-1", "r1") == DUPLICATE_IGNORED
    assert state.violations == ()


def test_wait_until_head_blocks_until_release() -> None:
    state = QueueState()
    state.apply_acquire(0, "sync-1", "r1")
    state.apply_acquire(1, "sync-2", "r2")

    finished = threading.Event()
    result: dict = {}

    def wait() -> None:
        result["head"] = state.wait_until_head("sync-2", "r2", timeout=5.0)
        finished.set()

    thread = threading.Thread(target=wait, daemon=True)
    thread.start()
    time.sleep(0.05)
    assert not finished.is_set()

    state.apply_release(2, "sync-1", "r1")
    assert finished.wait(2.0)
    assert result["head"] is True


def test_wait_until_head_timeout_returns_false() -> None:
    state = QueueState()
    state.apply_acquire(0, "sync-1", "r1")
    started = time.monotonic()
    assert state.wait_until_head("sync-2", "r2", timeout=0.05) is False
    assert time.monotonic() - started >= 0.05


def test_apply_dispatches_by_message_type() -> None:
    state = QueueState()
    acquire = ProtocolMessage(ACQUIRE, "R", "sync-1", "r1")
    release = ProtocolMessage(RELEASE, "R", "sync-1", "r1")
    assert state.apply(0, acquire) == ENQUEUED
    assert state.apply(1, release) == RELEASED


def test_apply_rejects_unknown_type() -> None:
    from types import SimpleNamespace

    state = QueueState()
    with pytest.raises(ValueError):
        state.apply(0, SimpleNamespace(type="OTHER"))


def test_sync_protocol_publishes_and_waits() -> None:
    log: list[ProtocolMessage] = []
    offsets: dict[str, int] = {}

    def publish(message: ProtocolMessage) -> int:
        offset = len(log)
        log.append(message)
        offsets[message.request_id] = offset
        # Entrega síncrona: emula o próprio consumidor vendo o registro.
        state.apply(offset, message)
        return offset

    state = QueueState()
    protocol = SyncProtocol("sync-1", publish, state=state)

    assert protocol.acquire("r1") == 0
    assert protocol.release("r1") == 1
    assert [message.type for message in log] == [ACQUIRE, RELEASE]
    assert state.entries() == ()


def test_wait_until_position_allows_first_k() -> None:
    state = QueueState()
    for index in range(1, 4):
        state.apply_acquire(index, f"sync-{index}", f"r{index}")

    assert state.wait_until_position("sync-1", "r1", limit=2, timeout=0.1)
    assert state.wait_until_position("sync-2", "r2", limit=2, timeout=0.1)
    assert not state.wait_until_position("sync-3", "r3", limit=2, timeout=0.05)


def test_semaphore_release_allowed_inside_window() -> None:
    state = QueueState(capacity=2)
    state.apply_acquire(0, "sync-1", "r1")
    state.apply_acquire(1, "sync-2", "r2")
    state.apply_acquire(2, "sync-3", "r3")

    # r2 não é cabeça, mas está na segunda posição autorizada (K=2).
    assert state.apply_release(3, "sync-2", "r2") == RELEASED
    assert [entry.request_id for entry in state.entries()] == ["r1", "r3"]
    assert state.violations == ()


def test_semaphore_release_outside_window_is_violation() -> None:
    state = QueueState(capacity=2)
    state.apply_acquire(0, "sync-1", "r1")
    state.apply_acquire(1, "sync-2", "r2")
    state.apply_acquire(2, "sync-3", "r3")

    assert state.apply_release(3, "sync-3", "r3") == INVARIANT_VIOLATION
    assert state.violations[0]["queue_position"] == 2
    assert state.violations[0]["capacity"] == 2
    assert [entry.request_id for entry in state.entries()] == ["r1", "r2"]


def test_semaphore_capacity_authorizes_k_and_blocks_the_rest() -> None:
    log: list[ProtocolMessage] = []
    state = QueueState()

    def publish(message: ProtocolMessage) -> int:
        offset = len(log)
        log.append(message)
        state.apply(offset, message)
        return offset

    protocols = [
        SyncProtocol(f"sync-{index}", publish, state=state, capacity=2)
        for index in range(1, 4)
    ]
    assert protocols[0].acquire("r1") == 0
    assert protocols[1].acquire("r2") == 1

    result: dict[str, int] = {}

    def acquire_third() -> None:
        result["offset"] = protocols[2].acquire("r3")

    thread = threading.Thread(target=acquire_third)
    thread.start()
    time.sleep(0.1)
    assert thread.is_alive(), "o terceiro pedido não deveria entrar com K=2"

    protocols[0].release("r1")
    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert result["offset"] == 2
