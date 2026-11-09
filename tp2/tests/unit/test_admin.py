"""Testes do cliente administrativo (sem Kafka real)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError, KafkaException

from harness.admin import describe_topic, describe_topic_configs


class _Future:
    def __init__(self, error: Exception | None, entries: dict | None = None) -> None:
        self._error = error
        self._entries = entries or {}

    def result(self, timeout: float | None = None) -> dict:
        if self._error is not None:
            raise self._error
        return self._entries


class _Admin:
    def __init__(self, futures: list[_Future]) -> None:
        self._futures = futures
        self.calls = 0

    def describe_configs(self, resources, request_timeout=None) -> dict:
        future = self._futures[min(self.calls, len(self._futures) - 1)]
        self.calls += 1
        return {resources[0]: future}


def test_describe_topic_configs_retries_unknown_topic(monkeypatch) -> None:
    """RF=3 pode tornar a metadata visível antes das configs."""
    monkeypatch.setattr("harness.admin.time.sleep", lambda _: None)
    error = KafkaException(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))
    admin = _Admin([
        _Future(error),
        _Future(None, {"cleanup.policy": SimpleNamespace(value="delete")}),
    ])

    configs = describe_topic_configs(admin, "r-events-test", attempts=3)

    assert configs == {"cleanup.policy": "delete"}
    assert admin.calls == 2


def test_describe_topic_configs_reports_after_attempts(monkeypatch) -> None:
    monkeypatch.setattr("harness.admin.time.sleep", lambda _: None)
    error = KafkaException(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))
    admin = _Admin([_Future(error)])

    with pytest.raises(RuntimeError, match="após 2 tentativas"):
        describe_topic_configs(admin, "r-events-test", attempts=2)
    assert admin.calls == 2


class _MetadataAdmin:
    """AdminClient falso cuja metadata demora a propagar."""

    def __init__(self, errors: int) -> None:
        self.errors = errors
        self.metadata_calls = 0
        self.config_admin = _Admin([
            _Future(None, {"cleanup.policy": SimpleNamespace(value="delete")}),
        ])

    def list_topics(self, topic=None, timeout=None):
        self.metadata_calls += 1
        if self.metadata_calls <= self.errors:
            entry = SimpleNamespace(
                error=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART),
                partitions={},
            )
        else:
            partition = SimpleNamespace(
                leader=1, replicas=[1, 2, 3], isrs=[1, 2, 3],
            )
            entry = SimpleNamespace(error=None, partitions={0: partition})
        return SimpleNamespace(topics={topic: entry})

    def describe_configs(self, resources, request_timeout=None) -> dict:
        return self.config_admin.describe_configs(resources, request_timeout)


def test_describe_topic_retries_metadata_propagation(monkeypatch) -> None:
    """A metadata pode sumir entre a criação e a verificação (RF=3)."""
    monkeypatch.setattr("harness.admin.time.sleep", lambda _: None)
    admin = _MetadataAdmin(errors=2)

    info = describe_topic(admin, "r-events-test", attempts=4)

    assert info["partitions"] == [{
        "id": 0, "leader": 1, "replicas": [1, 2, 3], "isr": [1, 2, 3],
    }]
    assert info["configs"] == {"cleanup.policy": "delete"}
    assert admin.metadata_calls == 3


def test_describe_topic_reports_after_attempts(monkeypatch) -> None:
    monkeypatch.setattr("harness.admin.time.sleep", lambda _: None)
    admin = _MetadataAdmin(errors=5)

    with pytest.raises(RuntimeError, match="não encontrado"):
        describe_topic(admin, "r-events-test", attempts=2)
    assert admin.metadata_calls == 2
