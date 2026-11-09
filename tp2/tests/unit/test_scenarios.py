"""Testes dos YAML de cenário (sem Kafka)."""

from __future__ import annotations

from pathlib import Path

import pytest

from common.config import ScenarioConfig
from harness.run_experiment import build_clients

SCENARIOS = Path(__file__).resolve().parents[2] / "src" / "harness" / "scenarios"


def _load(name: str) -> ScenarioConfig:
    return ScenarioConfig.from_yaml(SCENARIOS / f"{name}.yaml")


def test_all_scenarios_parse_and_obey_common_ranges() -> None:
    names = [
        "control-no-sync", "normal", "high-contention", "uneven-load", "scale-10",
        "extension-semaphore", "extension-semaphore-k3",
    ]
    for name in names:
        config = _load(name)
        assert config.name == name
        assert config.num_nodes >= 5
        assert config.num_clients >= 5
        assert 10 <= config.requests_per_client[0] <= config.requests_per_client[1] <= 50
        assert 0.2 <= config.cs_duration[0] <= config.cs_duration[1] <= 1.0
        assert config.think_time[0] >= 0
        assert config.repetitions >= 1
        assert config.capacity >= 1


def test_control_scenario_expects_violation_without_sync() -> None:
    config = _load("control-no-sync")
    assert config.sync_enabled is False
    assert config.expect_violation is True


def test_normal_and_high_contention_have_three_repetitions() -> None:
    for name in ("normal", "high-contention"):
        assert _load(name).repetitions >= 3


def test_uneven_load_overrides_single_client() -> None:
    config = _load("uneven-load")
    clients = build_clients(config, seed=config.seed)
    by_id = {client["client_id"]: client for client in clients}
    assert by_id["c1"]["requests"] == 40
    assert by_id["c1"]["think_time"] == (0.0, 0.2)
    others = [client for client in clients if client["client_id"] != "c1"]
    assert all(client["requests"] <= 15 for client in others)


def test_scale_ten_uses_one_node_per_client() -> None:
    config = _load("scale-10")
    clients = build_clients(config, seed=config.seed)
    assert config.num_nodes == 10
    assert len({client["node_index"] for client in clients}) == 10


def test_extension_semaphore_uses_capacity_two() -> None:
    config = _load("extension-semaphore")
    assert config.sync_enabled is True
    assert config.capacity == 2
    assert config.expect_violation is False


def test_extension_semaphore_k3_uses_capacity_three() -> None:
    config = _load("extension-semaphore-k3")
    assert config.sync_enabled is True
    assert config.capacity == 3
    assert config.expect_violation is False
