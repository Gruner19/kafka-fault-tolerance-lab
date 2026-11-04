"""Mensagens de protocolo do B6 e nomes canônicos de eventos de log."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, Mapping

# Tipos de mensagem publicados no log Kafka.
ACQUIRE = "ACQUIRE"
RELEASE = "RELEASE"
PROTOCOL_TYPES = frozenset({ACQUIRE, RELEASE})

# Eventos de log (nomes exatos exigidos pela spec, seção 6).
REQUEST = "REQUEST"
ENTER = "ENTER"
EXIT = "EXIT"
COMMITTED = "COMMITTED"
BEGIN = "BEGIN"
END = "END"
READY = "READY"
DUPLICATE_IGNORED = "DUPLICATE_IGNORED"
INVARIANT_VIOLATION = "INVARIANT_VIOLATION"
OVERLAP = "OVERLAP"

LOG_EVENTS = frozenset({
    REQUEST, ACQUIRE, ENTER, EXIT, RELEASE, COMMITTED,
    BEGIN, END, READY, DUPLICATE_IGNORED, INVARIANT_VIOLATION, OVERLAP,
})

DEFAULT_RESOURCE = "R"
PROTOCOL_FIELDS = ("type", "resource", "process_id", "request_id")


@dataclass(frozen=True)
class ProtocolMessage:
    """Uma linha do log Kafka: um ACQUIRE ou RELEASE de um recurso."""

    type: str
    resource: str
    process_id: str
    request_id: str

    def __post_init__(self) -> None:
        if self.type not in PROTOCOL_TYPES:
            raise ValueError(f"tipo de mensagem inválido: {self.type!r}")
        for field_name in ("resource", "process_id", "request_id"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"campo obrigatório vazio: {field_name}")

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"))

    def to_bytes(self) -> bytes:
        return self.to_json().encode("utf-8")

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> "ProtocolMessage":
        return cls(
            type=str(data["type"]),
            resource=str(data["resource"]),
            process_id=str(data["process_id"]),
            request_id=str(data["request_id"]),
        )

    @classmethod
    def from_json(cls, raw: str | bytes) -> "ProtocolMessage":
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            data = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"payload não é JSON válido: {exc}") from exc
        if not isinstance(data, dict):
            raise ValueError("payload de protocolo deve ser um objeto JSON")
        missing = [name for name in PROTOCOL_FIELDS if name not in data]
        if missing:
            raise ValueError(f"payload sem campos obrigatórios: {missing}")
        return cls.from_mapping(data)
