#!/usr/bin/env python3
"""Agrega os runs do TP2 em `aggregate.csv` e `report.md` (pt-BR).

Lê `output/<cenário>/<run>/summary.json` e `meta.json`, agrupa por cenário e
calcula mediana/mín/máx entre runs. Não recalcula a verificação: usa o campo
`verification_ok` gravado pelo harness.

Uso:
    python3 tp2/src/harness/aggregate.py --root tp2/output
    python3 tp2/src/harness/aggregate.py --root tp2/output --out tp2/output/agregado
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Optional


def read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def load_runs(root: Path) -> dict[str, list[dict]]:
    """Agrupa summaries por cenário, ignorando diretórios sem summary válido."""
    grouped: dict[str, list[dict]] = defaultdict(list)
    for summary_path in sorted(root.glob("*/*/summary.json")):
        summary = read_json(summary_path)
        if not summary or "scenario" not in summary:
            continue
        meta = read_json(summary_path.parent / "meta.json") or {}
        grouped[str(summary["scenario"])].append({
            "summary": summary,
            "meta": meta,
            "dir": str(summary_path.parent),
        })
    return dict(grouped)


def _median(values: Iterable[float]) -> Optional[float]:
    values = [float(value) for value in values]
    return statistics.median(values) if values else None


def _min(values: Iterable[float]) -> Optional[float]:
    values = [float(value) for value in values]
    return min(values) if values else None


def _max(values: Iterable[float]) -> Optional[float]:
    values = [float(value) for value in values]
    return max(values) if values else None


def _round(value: Optional[float], digits: int = 4) -> Optional[float]:
    return None if value is None else round(float(value), digits)


def aggregate_scenario(name: str, runs: list[dict]) -> dict:
    summaries = [run["summary"] for run in runs]
    waits = [summary["waits_s"] for summary in summaries]
    run_medians = [float(wait["median"]) for wait in waits if wait.get("median") is not None]
    run_mins = [float(wait["min"]) for wait in waits if wait.get("min") is not None]
    run_maxs = [float(wait["max"]) for wait in waits if wait.get("max") is not None]

    accesses: dict[str, int] = defaultdict(int)
    for summary in summaries:
        for client_id, count in (summary.get("accesses_per_client") or {}).items():
            accesses[str(client_id)] += int(count)

    observed = [
        float(summary["messages_per_entry_observed"])
        for summary in summaries
        if summary.get("messages_per_entry_observed") is not None
        and bool(summary.get("sync_enabled", True))
    ]
    theoretical = [
        int(summary["messages_per_entry_theoretical"])
        for summary in summaries
        if summary.get("messages_per_entry_theoretical") is not None
        and bool(summary.get("sync_enabled", True))
    ]

    durations = [float(summary["duration_s"]) for summary in summaries]
    load_durations = [float(summary["load_duration_s"]) for summary in summaries]
    total_requests = [int(summary["requests_total"]) for summary in summaries]
    completed = [int(summary["completed"]) for summary in summaries]

    nodes = {int(run["meta"].get("num_nodes", 0)) for run in runs}
    clients = {int(run["meta"].get("num_clients", 0)) for run in runs}
    first = summaries[0] if summaries else {}

    return {
        "scenario": name,
        "runs": len(runs),
        "passed": sum(1 for summary in summaries if summary.get("verification_ok")),
        "sync_enabled": bool(first.get("sync_enabled", True)),
        "capacity": int(first.get("capacity", 1)),
        "num_nodes": sorted(nodes),
        "num_clients": sorted(clients),
        "requests_total_median": _median(total_requests),
        "completed_median": _median(completed),
        "duration_s_median": _median(durations),
        "load_duration_s_median": _median(load_durations),
        "wait_median_across_runs_s": _median(run_medians),
        "wait_min_across_runs_s": _min(run_mins),
        "wait_max_across_runs_s": _max(run_maxs),
        "waits_per_run": run_medians,
        "messages_per_entry_observed_mean": _median(observed),
        "messages_per_entry_theoretical": theoretical[0] if theoretical else None,
        "accesses_per_client": dict(sorted(accesses.items())),
        "accesses_total": sum(accesses.values()),
        "run_ok": [bool(summary.get("verification_ok")) for summary in summaries],
        "seeds": [int(summary.get("seed", 0)) for summary in summaries],
        "run_ids": [summary.get("run_id") for summary in summaries],
        "run_dirs": [run["dir"] for run in runs],
    }


def aggregate_all(root: Path) -> list[dict]:
    grouped = load_runs(root)
    return [
        aggregate_scenario(name, runs)
        for name, runs in sorted(grouped.items())
    ]


CSV_FIELDS = [
    "scenario", "runs", "passed", "sync_enabled", "capacity", "num_nodes",
    "num_clients", "requests_total_median", "completed_median",
    "duration_s_median", "load_duration_s_median", "wait_median_across_runs_s",
    "wait_min_across_runs_s", "wait_max_across_runs_s",
    "messages_per_entry_observed_mean", "messages_per_entry_theoretical",
    "accesses_total",
]


def write_csv(path: Path, rows: list[dict]) -> None:
    def cell(value: Any) -> Any:
        if isinstance(value, list):
            return "|".join(str(item) for item in value)
        if isinstance(value, float):
            return _round(value)
        return value

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: cell(row[field]) for field in CSV_FIELDS})


def _fmt(value: Any, digits: int = 3) -> str:
    if value is None:
        return "—"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def write_report(path: Path, rows: list[dict], root: Path) -> None:
    lines: list[str] = []
    lines.append("# TP2 — B6 sobre Kafka: relatório agregado")
    lines.append("")
    lines.append(
        f"Fonte: `{root}`. Cada linha resume um cenário; tempos de espera usam "
        "`ENTER.mono_ns − REQUEST.mono_ns` medidos **no mesmo processo**."
    )
    lines.append("")
    lines.append("## Visão geral")
    lines.append("")
    lines.append(
        "| Cenário | Runs | PASS | K | Nós | Clientes | Pedidos (mediana) | "
        "Espera mediana (s) | Espera mín (s) | Espera máx (s) |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in rows:
        lines.append(
            "| {scenario} | {runs} | {passed} | {capacity} | {nodes} | {clients} | {requests} | "
            "{med} | {low} | {high} |".format(
                scenario=row["scenario"],
                runs=row["runs"],
                passed=row["passed"],
                capacity=row["capacity"],
                nodes=",".join(str(n) for n in row["num_nodes"]),
                clients=",".join(str(c) for c in row["num_clients"]),
                requests=_fmt(row["requests_total_median"], 1),
                med=_fmt(row["wait_median_across_runs_s"]),
                low=_fmt(row["wait_min_across_runs_s"]),
                high=_fmt(row["wait_max_across_runs_s"]),
            )
        )
    lines.append("")
    lines.append(
        "`K=1` corresponde ao mutex obrigatório sobre `R`; `K>1` identifica a "
        "extensão semáforo, executada em etapa separada."
    )
    lines.append("")

    lines.append("## Mensagens por entrada")
    lines.append("")
    lines.append(
        "Convenção: 1 publicação = 1 mensagem; cada entrega a cada consumidor = "
        "1 mensagem (inclui o próprio publicador). Teórico do B6: **2N + 2** "
        "(2 publicações + 2N entregas)."
    )
    lines.append("")
    lines.append("| Cenário | N | Observado (média) | Teórico 2N+2 |")
    lines.append("|---|---:|---:|---:|")
    for row in rows:
        if not row["sync_enabled"]:
            continue
        nodes = row["num_nodes"][0] if len(row["num_nodes"]) == 1 else None
        lines.append(
            "| {scenario} | {nodes} | {obs} | {theory} |".format(
                scenario=row["scenario"],
                nodes=_fmt(nodes, 0),
                obs=_fmt(row["messages_per_entry_observed_mean"]),
                theory=_fmt(row["messages_per_entry_theoretical"], 0),
            )
        )
    lines.append("")

    lines.append("## Acessos por cliente")
    lines.append("")
    lines.append("| Cenário | Cliente | Acessos |")
    lines.append("|---|---|---:|")
    for row in rows:
        for client_id, count in row["accesses_per_client"].items():
            lines.append(f"| {row['scenario']} | {client_id} | {count} |")
    lines.append("")

    lines.append("## Runs e sementes")
    lines.append("")
    lines.append("| Cenário | Run | Semente | PASS |")
    lines.append("|---|---|---:|:---:|")
    for row in rows:
        for run_id, seed, ok in zip(row["run_ids"], row["seeds"], row["run_ok"]):
            lines.append(f"| {row['scenario']} | `{run_id}` | {seed} | {'sim' if ok else 'não'} |")
    lines.append("")

    control = [row for row in rows if not row["sync_enabled"]]
    if control:
        lines.append("## Modo control")
        lines.append("")
        lines.append(
            "No modo control (`sync_enabled=false`) a corrida é considerada "
            "**bem-sucedida quando V1 falha de propósito**: o observador deve "
            "registrar `overlaps > 0`, provando que a instrumentação detecta "
            "acessos simultâneos."
        )
        lines.append("")
        for row in control:
            lines.append(
                f"- `{row['scenario']}`: {row['passed']}/{row['runs']} runs com "
                "sobreposição detectada."
            )
        lines.append("")

    lines.append("## Limitações")
    lines.append("")
    lines.append(
        "- Os experimentos são evidência, não prova: a exclusão mútua decorre do "
        "argumento da fila replicada (seção 3.3 da spec)."
    )
    lines.append(
        "- `aggregate.csv` contém os mesmos números em formato de máquina; "
        "`meta.json` de cada run registra versões, sementes e configuração do tópico."
    )
    lines.append("- Não comparar runs com duração de seção crítica ou carga diferentes.")
    lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Agrega resultados do TP2")
    parser.add_argument("--root", default="output", help="diretório com <cenário>/<run>/")
    parser.add_argument("--out", default=None, help="diretório de saída (padrão: <root>/agregado)")
    return parser


def main(argv: Optional[list] = None) -> int:
    args = build_parser().parse_args(argv)
    root = Path(args.root)
    out_dir = Path(args.out) if args.out else root / "agregado"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = aggregate_all(root)
    if not rows:
        print(f"nenhum summary.json encontrado em {root}")
        return 2

    write_csv(out_dir / "aggregate.csv", rows)
    write_report(out_dir / "report.md", rows, root)

    print(json.dumps({
        "scenarios": len(rows),
        "runs": sum(row["runs"] for row in rows),
        "passed": sum(row["passed"] for row in rows),
        "csv": str(out_dir / "aggregate.csv"),
        "report": str(out_dir / "report.md"),
    }, indent=2, ensure_ascii=False))
    return 0 if all(row["passed"] == row["runs"] for row in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
