"""Seção crítica simulada, substituível no TP3 pelo acesso ao Cluster Store."""

from __future__ import annotations

import time
from typing import Protocol


class CriticalSection(Protocol):
    def execute(self, duration_s: float) -> None:
        ...


class SimulatedCriticalSection:
    """Acesso simulado: apenas dorme o tempo sorteado (0,2–1,0 s no cenário)."""

    def execute(self, duration_s: float) -> None:
        time.sleep(max(0.0, duration_s))
