"""Configuração de cenário e derivação determinística de sementes."""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Optional

import yaml

DEFAULT_BOOTSTRAP = "localhost:9092"


def derive_seed(base_seed: int, actor: str) -> int:
    """Semente estável entre execuções (não usa hash() aleatório do Python)."""
    digest = hashlib.sha256(f"{base_seed}:{actor}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


def rng_for(base_seed: int, actor: str) -> random.Random:
    return random.Random(derive_seed(base_seed, actor))


def _pair(value: Any, name: str) -> tuple:
    if isinstance(value, (int, float)):
        return (value, value)
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise ValueError(f"{name} deve ser um par [min, max]")
    low, high = value
    if low > high:
        raise ValueError(f"{name}: min > max")
    return (low, high)


@dataclass(frozen=True)
class ScenarioConfig:
    """Parâmetros comuns da seção 8 da spec (tempos em segundos)."""

    name: str
    num_nodes: int = 5
    num_clients: int = 5
    requests_per_client: tuple = (10, 50)
    cs_duration: tuple = (0.2, 1.0)
    think_time: tuple = (1.0, 5.0)
    seed: int = 0
    sync_enabled: bool = True
    repetitions: int = 1
    capacity: int = 1
    bootstrap: str = DEFAULT_BOOTSTRAP
    replication_factor: int = 1
    min_insync_replicas: int = 1
    retention_ms: int = 86_400_000
    expect_violation: bool = False
    observer_port: int = 7100
    node_port_base: int = 7001
    clients: tuple = ()
    raw: dict = field(default_factory=dict, compare=False, repr=False)

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("cenário sem nome")
        if self.num_nodes < 1 or self.num_clients < 1:
            raise ValueError("num_nodes e num_clients devem ser >= 1")
        if self.repetitions < 1:
            raise ValueError("repetitions deve ser >= 1")
        if self.capacity < 1:
            raise ValueError("capacity deve ser >= 1")

        object.__setattr__(
            self, "requests_per_client", _pair(self.requests_per_client, "requests_per_client"),
        )
        for name in ("cs_duration", "think_time"):
            object.__setattr__(self, name, tuple(float(v) for v in _pair(getattr(self, name), name)))
        if self.cs_duration[0] < 0 or self.cs_duration[1] < 0:
            raise ValueError("cs_duration não pode ser negativa")
        if self.think_time[0] < 0 or self.think_time[1] < 0:
            raise ValueError("think_time não pode ser negativo")
        if self.replication_factor < 1:
            raise ValueError("replication_factor deve ser >= 1")
        if self.min_insync_replicas < 1:
            raise ValueError("min_insync_replicas deve ser >= 1")

    @classmethod
    def from_dict(cls, data: dict) -> "ScenarioConfig":
        if not isinstance(data, dict):
            raise ValueError("cenário deve ser um objeto YAML")
        known = {
            "name", "num_nodes", "num_clients", "requests_per_client",
            "cs_duration", "think_time", "seed", "sync_enabled", "repetitions",
            "capacity", "bootstrap", "replication_factor", "min_insync_replicas",
            "retention_ms", "expect_violation", "observer_port", "node_port_base",
            "clients",
        }
        values = {k: v for k, v in data.items() if k in known}
        extras = {k: v for k, v in data.items() if k not in known}
        values.setdefault("name", "sem-nome")
        config = cls(**values, raw=extras)
        if "clients" in data and data["clients"]:
            object.__setattr__(config, "clients", tuple(dict(item) for item in data["clients"]))
        return config

    @classmethod
    def from_yaml(cls, path: str | Path) -> "ScenarioConfig":
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        return cls.from_dict(payload)

    def with_overrides(self, **overrides: Any) -> "ScenarioConfig":
        return replace(self, **overrides)


def load_scenario(path: str | Path) -> ScenarioConfig:
    return ScenarioConfig.from_yaml(path)
