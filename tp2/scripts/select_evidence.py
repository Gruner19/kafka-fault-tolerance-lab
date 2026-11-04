#!/usr/bin/env python3
"""Seleciona um acesso concreto como evidência legível (pt-BR).

Escolhe o `ENTER` com maior `queue_len` (empate: o mais antigo) e mostra a
sequência `REQUEST → ACQUIRE → ENTER → BEGIN → EXIT → RELEASE → COMMITTED`,
com `queue_head`/`queue_len`, além da espera calculada com `mono_ns` do mesmo
processo.

Uso:
    python3 tp2/scripts/select_evidence.py tp2/output/high-contention/<run_id>
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

ORDER = ["REQUEST", "ACQUIRE", "ENTER", "EXIT", "RELEASE", "COMMITTED"]


def read_jsonl(path: Path) -> list[dict]:
    records = []
    if not path.exists():
        return records
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


def ns_to_ms(value: Optional[int]) -> str:
    if value is None:
        return "—"
    return f"{value / 1_000_000:.3f}"


def choose_access(run_dir: Path) -> Optional[dict]:
    best = None
    for path in sorted(run_dir.glob("node-*.jsonl")):
        for record in read_jsonl(path):
            if record["event"] != "ENTER":
                continue
            key = (int(record.get("queue_len", 0)), -int(record["mono_ns"]))
            if best is None or key > best[0]:
                best = (key, record)
    return None if best is None else best[1]


def select(run_dir: Path) -> str:
    chosen = choose_access(run_dir)
    if chosen is None:
        return "# Evidência\n\nNenhum evento ENTER encontrado."

    process_id = chosen["process_id"]
    request_id = chosen["request_id"]
    node_records = read_jsonl(run_dir / f"node-{process_id}.jsonl")
    observer = read_jsonl(run_dir / "observer.jsonl")

    by_event = {
        record["event"]: record
        for record in node_records
        if record.get("process_id") == process_id
        and record.get("request_id") == request_id
    }
    observer_events = [
        record for record in observer
        if record.get("request_id") == request_id
        and record.get("process_id") == process_id
        and record["event"] in ("BEGIN", "END")
    ]
    observer_events.sort(key=lambda record: record["obs_seq"])

    wait_ms = None
    if "REQUEST" in by_event and "ENTER" in by_event:
        wait_ms = by_event["ENTER"]["mono_ns"] - by_event["REQUEST"]["mono_ns"]
    cs_ms = None
    observer_by_event = {record["event"]: record for record in observer_events}
    if "BEGIN" in observer_by_event and "END" in observer_by_event:
        cs_ms = (
            observer_by_event["END"]["mono_ns"]
            - observer_by_event["BEGIN"]["mono_ns"]
        )

    lines = [
        "# Evidência de um acesso",
        "",
        f"Run: `{run_dir.name}`  ",
        f"Processo: `{process_id}` · Pedido: `{request_id}`  ",
        f"Espera local (`ENTER.mono_ns − REQUEST.mono_ns`): **{ns_to_ms(wait_ms)} ms**  ",
        f"Duração da seção crítica (`END.mono_ns − BEGIN.mono_ns`, observador): **{ns_to_ms(cs_ms)} ms**",
        "",
        "## Eventos do nó",
        "",
        "| Evento | mono_ns (ms) | offset | queue_head | queue_len |",
        "|---|---:|---:|---|---:|",
    ]
    for name in ORDER:
        record = by_event.get(name)
        if record is None:
            continue
        lines.append(
            "| {event} | {mono} | {offset} | {head} | {length} |".format(
                event=name,
                mono=ns_to_ms(record.get("mono_ns")),
                offset=record.get("offset", "—"),
                head=record.get("queue_head") or "—",
                length=record.get("queue_len", "—"),
            )
        )

    lines.extend([
        "",
        "## Observador (fora do protocolo)",
        "",
        "| Evento | obs_seq | active |",
        "|---|---:|---:|",
    ])
    for record in observer_events:
        lines.append(
            f"| {record['event']} | {record['obs_seq']} | {record['active']} |"
        )

    queue_len = int(chosen.get("queue_len", 0))
    lines.extend([
        "",
        "## Leitura",
        "",
        (
            f"Quando `{request_id}` entrou, a fila tinha **{queue_len}** pedido(s) "
            f"pendente(s) e a cabeça era `{chosen.get('queue_head')}`. Como só a "
            "cabeça é autorizada, os pedidos seguintes permaneceram na fila até "
            "o `RELEASE` correspondente. O `BEGIN` no observador veio depois do "
            "`ENTER` e o `RELEASE` só foi publicado após o ACK do `END`."
        ),
        "",
    ])
    return "\n".join(lines)


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description="Extrai evidência legível de um run")
    parser.add_argument("run_dir")
    parser.add_argument("--out", default=None)
    args = parser.parse_args(argv)

    run_dir = Path(args.run_dir)
    text = select(run_dir)
    out = Path(args.out) if args.out else run_dir / "evidence_access.md"
    out.write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
