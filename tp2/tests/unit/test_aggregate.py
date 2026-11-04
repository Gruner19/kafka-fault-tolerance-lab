"""Testes da agregação de resultados (sem Kafka)."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from harness.aggregate import aggregate_all, main


def _write_run(
    root: Path,
    scenario: str,
    run_id: str,
    *,
    seed: int,
    wait_median: float,
    observed: float | None,
    theory: int | None,
    sync: bool = True,
    ok: bool = True,
) -> None:
    run_dir = root / scenario / run_id
    run_dir.mkdir(parents=True)
    summary = {
        "scenario": scenario,
        "run_id": run_id,
        "repetition": 1,
        "seed": seed,
        "sync_enabled": sync,
        "capacity": 1,
        "requests_total": 10,
        "completed": 10,
        "duration_s": 12.0,
        "load_duration_s": 9.0,
        "waits_s": {"count": 10, "min": wait_median / 2, "median": wait_median, "max": wait_median * 2},
        "accesses_per_client": {"c1": 6, "c2": 4},
        "cs_duration_s": [0.2, 1.0],
        "think_time_s": [1.0, 5.0],
        "protocol_published": {"sync-1": 10} if sync else {},
        "protocol_delivered": {"sync-1": 22} if sync else {},
        "messages_per_entry_observed": observed,
        "messages_per_entry_theoretical": theory,
        "topic": f"r-events-{run_id}",
        "verification_ok": ok,
    }
    (run_dir / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (run_dir / "meta.json").write_text(
        json.dumps({"num_nodes": 5, "num_clients": 5}), encoding="utf-8",
    )


def test_aggregate_groups_runs_and_uses_median_of_medians(tmp_path: Path) -> None:
    _write_run(tmp_path, "normal", "r1", seed=1, wait_median=1.0, observed=12.0, theory=12)
    _write_run(tmp_path, "normal", "r2", seed=2, wait_median=3.0, observed=12.0, theory=12)
    _write_run(tmp_path, "normal", "r3", seed=3, wait_median=2.0, observed=12.0, theory=12)

    rows = aggregate_all(tmp_path)
    assert len(rows) == 1
    row = rows[0]
    assert row["runs"] == 3
    assert row["passed"] == 3
    assert row["wait_median_across_runs_s"] == 2.0
    assert row["wait_min_across_runs_s"] == 0.5  # mínimo dos mínimos (1.0/2)
    assert row["wait_max_across_runs_s"] == 6.0  # máximo dos máximos (3.0*2)
    assert row["accesses_per_client"] == {"c1": 18, "c2": 12}
    assert row["messages_per_entry_observed_mean"] == 12.0


def test_control_runs_do_not_contribute_messages_per_entry(tmp_path: Path) -> None:
    _write_run(
        tmp_path, "control-no-sync", "r1", seed=9,
        wait_median=0.001, observed=None, theory=None, sync=False,
    )
    rows = aggregate_all(tmp_path)
    assert rows[0]["messages_per_entry_observed_mean"] is None
    assert rows[0]["messages_per_entry_theoretical"] is None
    assert rows[0]["sync_enabled"] is False


def test_cli_writes_csv_and_report(tmp_path: Path) -> None:
    _write_run(tmp_path, "normal", "r1", seed=1, wait_median=1.0, observed=12.0, theory=12)
    out = tmp_path / "agregado"
    code = main(["--root", str(tmp_path), "--out", str(out)])
    assert code == 0
    assert (out / "aggregate.csv").exists()
    assert (out / "report.md").exists()

    with (out / "aggregate.csv").open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["scenario"] == "normal"
    assert rows[0]["num_nodes"] == "5"
    text = (out / "report.md").read_text(encoding="utf-8")
    assert "relatório agregado" in text
    assert "2N + 2" in text
