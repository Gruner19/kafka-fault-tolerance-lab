"""Testes de configuração, sementes e mensagens."""

from __future__ import annotations

import json

import pytest

from common.config import ScenarioConfig, derive_seed, load_scenario, rng_for
from common.messages import ACQUIRE, RELEASE, ProtocolMessage


def test_scenario_defaults() -> None:
    scenario = ScenarioConfig.from_dict({"name": "normal"})
    assert scenario.num_nodes == 5
    assert scenario.requests_per_client == (10, 50)
    assert scenario.cs_duration == (0.2, 1.0)
    assert scenario.sync_enabled is True


def test_scenario_pairs_and_overrides() -> None:
    scenario = ScenarioConfig.from_dict({
        "name": "high-contention",
        "num_nodes": 10,
        "think_time": [0, 0],
        "cs_duration": 0.5,
        "requests_per_client": 20,
        "seed": 1234,
        "sync_enabled": False,
        "expect_violation": True,
    })
    assert scenario.num_nodes == 10
    assert scenario.think_time == (0.0, 0.0)
    assert scenario.cs_duration == (0.5, 0.5)
    assert scenario.requests_per_client == (20, 20)
    assert scenario.sync_enabled is False
    assert scenario.expect_violation is True


def test_scenario_rejects_invalid_ranges() -> None:
    with pytest.raises(ValueError):
        ScenarioConfig.from_dict({"name": "x", "cs_duration": [1.0, 0.2]})
    with pytest.raises(ValueError):
        ScenarioConfig.from_dict({"name": "x", "num_nodes": 0})


def test_scenario_keeps_unknown_keys_as_raw() -> None:
    scenario = ScenarioConfig.from_dict({"name": "x", "prometheus_url": "http://localhost:9090"})
    assert scenario.raw == {"prometheus_url": "http://localhost:9090"}


def test_load_scenario_from_yaml(tmp_path) -> None:
    path = tmp_path / "normal.yaml"
    path.write_text(
        "name: normal\n"
        "num_nodes: 5\n"
        "num_clients: 5\n"
        "requests_per_client: [10, 50]\n"
        "seed: 42\n",
        encoding="utf-8",
    )
    scenario = load_scenario(path)
    assert scenario.name == "normal"
    assert scenario.seed == 42


def test_seed_derivation_is_stable_and_actor_specific() -> None:
    assert derive_seed(7, "sync-1") == derive_seed(7, "sync-1")
    assert derive_seed(7, "sync-1") != derive_seed(7, "sync-2")
    first = rng_for(7, "sync-1").random()
    second = rng_for(7, "sync-1").random()
    assert first == second


def test_protocol_message_roundtrip() -> None:
    message = ProtocolMessage(ACQUIRE, "R", "sync-3", "c3-0017")
    decoded = ProtocolMessage.from_json(message.to_json())
    assert decoded == message
    assert json.loads(message.to_json())["type"] == ACQUIRE


def test_protocol_message_rejects_invalid_payloads() -> None:
    with pytest.raises(ValueError):
        ProtocolMessage("LOCK", "R", "sync-1", "r1")
    with pytest.raises(ValueError):
        ProtocolMessage(ACQUIRE, "R", "", "r1")
    with pytest.raises(ValueError):
        ProtocolMessage.from_json(b'{"type":"ACQUIRE"}')
    with pytest.raises(ValueError):
        ProtocolMessage.from_json(b"not-json")


def test_release_message_serializes_expected_shape() -> None:
    payload = json.loads(ProtocolMessage(RELEASE, "R", "sync-1", "r1").to_json())
    assert payload == {
        "type": RELEASE,
        "resource": "R",
        "process_id": "sync-1",
        "request_id": "r1",
    }
