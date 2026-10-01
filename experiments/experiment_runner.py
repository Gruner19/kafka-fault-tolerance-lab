#!/usr/bin/env python3
"""
Runner de experimentos de tolerancia a fallos para el clúster Kafka.

Qué hace:
  1. Carga un escenario YAML (brokers, topic, fallos, tiempos).
  2. Lanza el productor y consumidor de pedidos.
  3. Monitorea métricas en tiempo real vía la API de Prometheus
     (under-replicated partitions, offline partitions, active controller,
      ISR shrinks/grows, throughput, consumer lag).
  4. Inyecta los fallos programados (caída de broker, reinicio total, etc.)
     vía SSH + systemctl.
  5. Calcula MTTD (tiempo en detectar) y MTTR (tiempo en recuperar),
     cuantifica pérdida de mensajes y lag pico.
  6. Genera:  output/<escenario>/raw_series.csv
              output/<escenario>/summary.json
              output/<escenario>/report.html   (con gráficos matplotlib)

Requisitos:
  - Túneles SSH activos (scripts/tunnels.sh) → prometheus_url localhost:9090
  - Cliente de Kafka alcanzable (producer/consumer).
  - Dependencias: pip install -r scripts/app/requirements.txt

Uso:
  python3 experiments/experiment_runner.py \
      --scenario experiments/scenarios/broker-failure.yaml
"""
from __future__ import annotations

import argparse
import base64
import csv
import io
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests
import yaml

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    HAS_MPL = True
except Exception:  # noqa: BLE001
    HAS_MPL = False

ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "scripts" / "app"
DEFAULT_OUT = ROOT / "output"


# --------------------------------------------------------------------------
# Cliente Prometheus
# --------------------------------------------------------------------------
class PrometheusClient:
    METRICS = {
        "underreplicated": "kafka_server_replicamanager_underreplicatedpartitions",
        "offline": "kafka_controller_kafkacontroller_offlinepartitionscount",
        "active_controller": "kafka_controller_kafkacontroller_activecontrollercount",
        "isr_shrinks": "kafka_server_replicamanager_isrshrinks_total",
        "isr_grows": "kafka_server_replicamanager_isrgrows_total",
        "messages_in": "kafka_server_brokertopicmetrics_messagesin_total",
    }

    def __init__(self, base_url: str):
        self.base = base_url.rstrip("/")
        self.session = requests.Session()
        self.errors: list[str] = []

    def query_single(self, metric: str, labels: Optional[dict] = None) -> list[tuple[float, float]]:
        """Devuelve la última muestra de cada serie de una métrica."""
        params = {
            "query": metric,
        }
        try:
            r = self.session.get(f"{self.base}/api/v1/query", params=params, timeout=5)
            r.raise_for_status()
            data = r.json()
        except Exception as exc:  # noqa: BLE001
            message = f"Prometheus query failed for {metric}: {exc}"
            self.errors.append(message)
            return []
        if data.get("status") != "success":
            message = f"Prometheus query returned an invalid status for {metric}: {data}"
            self.errors.append(message)
            return []
        series = []
        for s in data.get("data", {}).get("result", []):
            # filtrar por etiquetas si se piden
            metrics_labels = s.get("metric", {})
            if labels and not all(metrics_labels.get(k) == v for k, v in labels.items()):
                continue
            value = s.get("value")
            if value and len(value) == 2:
                series.append((float(value[0]), float(value[1])))
        series.sort()
        return series

    def sum_metric(self, metric: str) -> float:
        """Suma el aporte de la última muestra de cada serie (para throughput)."""
        s = self.query_single(metric)
        return sum(v for _, v in s) if s else 0.0

    def latest(self, metric: str, labels: Optional[dict] = None) -> float:
        s = self.query_single(metric, labels)
        return s[-1][1] if s else 0.0


def run_preflight(scenario: Scenario) -> dict:
    """Comprueba dependencias externas sin iniciar productores ni inyectar fallos."""
    checks: list[dict[str, Any]] = []

    def add_check(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": ok, "detail": detail})
        marker = "OK" if ok else "FAIL"
        print(f"[{marker}] {name}: {detail}")

    prom = PrometheusClient(scenario.prometheus_url)
    try:
        ready = prom.session.get(f"{prom.base}/-/ready", timeout=5)
        add_check("prometheus_ready", ready.ok, f"HTTP {ready.status_code}")
    except requests.RequestException as exc:
        add_check("prometheus_ready", False, str(exc))

    try:
        targets = prom.session.get(
            f"{prom.base}/api/v1/targets", timeout=5,
        )
        payload = targets.json()
        active = payload.get("data", {}).get("activeTargets", [])
        failed = [target for target in active if target.get("health") != "up"]
        failed_details = "; ".join(
            f"{target.get('labels', {}).get('job', 'unknown')}: "
            f"{target.get('lastError', 'sin detalle')}"
            for target in failed
        )
        add_check(
            "prometheus_targets",
            targets.ok and not failed,
            f"{len(active)} activos, {len(failed)} con fallo"
            + (f" ({failed_details})" if failed_details else ""),
        )
    except (requests.RequestException, ValueError) as exc:
        add_check("prometheus_targets", False, str(exc))

    for name, expected in (
        ("active_controller", 1),
        ("underreplicated", 0),
        ("offline", 0),
    ):
        values = [value for _, value in prom.query_single(PrometheusClient.METRICS[name])]
        observed = max(values, default=None)
        add_check(
            f"metric_{name}",
            observed == expected,
            f"observado={observed}, esperado={expected}",
        )

    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": scenario.bootstrap})
        metadata = admin.list_topics(topic=scenario.topic, timeout=10)
        topic_metadata = metadata.topics.get(scenario.topic)
        partitions = list(topic_metadata.partitions.values()) if topic_metadata else []
        replication = {len(partition.replicas) for partition in partitions}
        expected_partitions = 3
        expected_replication = 3
        add_check(
            "kafka_topic",
            topic_metadata is not None
            and len(partitions) == expected_partitions
            and replication == {expected_replication},
            f"particiones={len(partitions)}, replicacion={sorted(replication)}",
        )
    except Exception as exc:  # noqa: BLE001
        add_check("kafka_topic", False, str(exc))

    result = {
        "scenario": scenario.name,
        "bootstrap": scenario.bootstrap,
        "prometheus_url": scenario.prometheus_url,
        "checks": checks,
        "ok": all(check["ok"] for check in checks),
        "prometheus_errors": prom.errors[:20],
    }
    return result


# --------------------------------------------------------------------------
# Inyección de fallos vía SSH
# --------------------------------------------------------------------------
class SshFaultInjector:
    def __init__(self, key: str, user: str):
        self.key = key
        self.user = user

    def _ssh(self, host: str, cmd: str) -> None:
        argv = [
            "ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "ConnectTimeout=15",
        ]
        if self.key:
            argv.extend(["-i", self.key])
        argv.extend([f"{self.user}@{host}", cmd])
        subprocess.run(argv, check=True, timeout=120)

    def stop_broker(self, host: str) -> None:
        print(f"  [fault] detener broker en {host}", flush=True)
        self._ssh(host, "sudo systemctl stop kafka")

    def start_broker(self, host: str) -> None:
        print(f"  [fault] iniciar broker en {host}", flush=True)
        self._ssh(host, "sudo systemctl start kafka")

    def stop_all(self, hosts: list[str]) -> None:
        for h in hosts:
            self.stop_broker(h)

    def start_all(self, hosts: list[str]) -> None:
        for h in hosts:
            self.start_broker(h)


# --------------------------------------------------------------------------
# Modelo de escenario y ejecución
# --------------------------------------------------------------------------
@dataclass
class Fault:
    kind: str
    at: float          # segundos desde el inicio
    broker: Optional[str] = None
    down_for: float = 45.0
    restart: bool = True
    duration: float = 0.0    # para rebalance: nada que detener


@dataclass
class Scenario:
    name: str
    topic: str = "orders"
    bootstrap: str = "kafka-1:9092"
    prometheus_url: str = "http://localhost:9090"
    producer_rate: float = 5.0
    baseline_seconds: float = 60.0
    recovery_seconds: float = 60.0
    consumer_group: str = "order-processor-group"
    brokers: list[dict] = field(default_factory=list)
    ssh_key: str = "/home/gruner-sanchez/Descargas/SSH/kafka-cluster-key.pem"
    ssh_user: str = "ubuntu"
    faults: list[Fault] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict) -> "Scenario":
        brokers = d.get("brokers", [])
        faults = []
        for f in d.get("faults", []):
            faults.append(Fault(**f))
        return cls(
            name=d["name"],
            topic=d.get("topic", "orders"),
            bootstrap=d.get("bootstrap", "kafka-1:9092"),
            prometheus_url=d.get("prometheus_url", "http://localhost:9090"),
            producer_rate=d.get("producer_rate", 5.0),
            baseline_seconds=d.get("baseline_seconds", 60.0),
            recovery_seconds=d.get("recovery_seconds", 60.0),
            consumer_group=d.get("consumer_group", "order-processor-group"),
            brokers=brokers,
            ssh_key=d.get("ssh_key", "/home/gruner-sanchez/Descargas/SSH/kafka-cluster-key.pem"),
            ssh_user=d.get("ssh_user", "ubuntu"),
            faults=faults,
        )


class MetricsRecorder:
    """Recolecta muestras de métricas y las persiste en CSV incrementally."""

    def __init__(self, prom: PrometheusClient, csv_path: Path):
        self.prom = prom
        self.csv_path = csv_path
        self.rows: list[dict] = []
        self.columns = list(prom.METRICS.keys()) + ["consumer_lag", "phase"]
        self._fh = csv_path.open("w", newline="")
        self._writer = csv.DictWriter(self._fh, fieldnames=["ts"] + self.columns)
        self._writer.writeheader()

    def sample(self, phase: str, lag: float = 0.0) -> dict:
        row = {"ts": time.time(), "phase": phase}
        for key, metric in PrometheusClient.METRICS.items():
            row[key] = self.prom.latest(metric)
        row["consumer_lag"] = lag
        self.rows.append(row)
        self._writer.writerow(row)
        self._fh.flush()
        return row

    def close(self):
        self._fh.close()

    def as_pandas_like(self):
        return self.rows


# --------------------------------------------------------------------------
# Análisis
# --------------------------------------------------------------------------
def compute_mttd_mttr(rows: list[dict], fault_time: float) -> dict:
    """
    MTTD: primer momento en que underreplicated > 0 tras el fault (detección).
    MTTR: último momento en que underreplicated > 0 (recuperación) → vuelta a 0.
    """
    after = [r for r in rows if r["ts"] >= fault_time - 1]
    crash_detected = next((r for r in after if r["underreplicated"] > 0), None)
    # recuperación: la primera muestra (tras detección) con underreplicated == 0
    recovered = None
    triggered = False
    for r in after:
        if r["underreplicated"] > 0:
            triggered = True
        elif triggered and r["underreplicated"] == 0:
            recovered = r
            break
    return {
        "mttd_s": round((crash_detected["ts"] - fault_time), 2) if crash_detected else None,
        "mttr_s": round((recovered["ts"] - fault_time), 2) if recovered else None,
        "peak_underreplicated": max((r["underreplicated"] for r in after), default=0),
        "peak_offline": max((r["offline"] for r in after), default=0),
    }


def _percentile(values: list[float], percentile: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * percentile / 100
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    weight = index - lower
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * weight, 2)


def summarize_phases(rows: list[dict]) -> dict:
    """Resume estabilidad y degradación por fase del experimento."""
    phases: dict[str, list[dict]] = {}
    for row in rows:
        phases.setdefault(row["phase"], []).append(row)

    result = {}
    for phase, phase_rows in phases.items():
        lag = [float(row["consumer_lag"]) for row in phase_rows]
        under = [float(row["underreplicated"]) for row in phase_rows]
        offline = [float(row["offline"]) for row in phase_rows]
        controller = [float(row["active_controller"]) for row in phase_rows]
        result[phase] = {
            "samples": len(phase_rows),
            "lag_avg": round(sum(lag) / len(lag), 2) if lag else None,
            "lag_peak": max(lag, default=None),
            "lag_p95": _percentile(lag, 95),
            "underreplicated_peak": max(under, default=None),
            "offline_peak": max(offline, default=None),
            "controller_min": min(controller, default=None),
        }
    return result


# --------------------------------------------------------------------------
# Salida: reporte HTML
# --------------------------------------------------------------------------
def make_chart(rows: list[dict], x: str, y: str, title: str) -> str:
    xs = [r[x] - rows[0]["ts"] for r in rows]
    ys = [r[y] for r in rows]
    fig, ax = plt.subplots(figsize=(7, 3))
    ax.plot(xs, ys, marker=".", markersize=2, linewidth=1)
    ax.set_xlabel("segundos desde inicio")
    ax.set_ylabel(y)
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=90)
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


def render_report(scenario: Scenario, rows: list[dict], analysis: dict,
                  summary: dict, out: Path) -> None:
    charts = {}
    if HAS_MPL:
        charts["under"] = make_chart(rows, "ts", "underreplicated",
                                     "Under-replicated partitions (tolerancia a fallos)")
        charts["offline"] = make_chart(rows, "ts", "offline", "Offline partitions")
        charts["lag"] = make_chart(rows, "ts", "consumer_lag", "Consumer lag")
        charts["shrink_grow"] = make_chart(rows, "ts", "isr_shrinks", "ISR shrinks (acumulado)")

    metrics_tbl = "\n".join(
        "".join(
            f"<td>{v:g}</td>" if isinstance(v, (int, float)) else f"<td>{v}</td>"
            for v in (r["ts"] - rows[0]["ts"], r["phase"], r["underreplicated"],
                      r["offline"], r["active_controller"], r["consumer_lag"])
        )
        for r in rows[:: max(1, len(rows) // 50)]
    )

    phase_rows = "\n".join(
        "<tr>"
        f"<td>{phase}</td><td>{stats['samples']}</td>"
        f"<td>{stats['lag_avg']}</td><td>{stats['lag_peak']}</td>"
        f"<td>{stats['lag_p95']}</td><td>{stats['underreplicated_peak']}</td>"
        f"<td>{stats['offline_peak']}</td><td>{stats['controller_min']}</td>"
        "</tr>"
        for phase, stats in summary.get("phase_statistics", {}).items()
    )
    event_rows = "\n".join(
        "<tr>"
        f"<td>{event.get('type')}</td><td>{event.get('fault_index', '')}</td>"
        f"<td>{event.get('elapsed_s')}</td><td>{event.get('broker', '')}</td>"
        "</tr>"
        for event in summary.get("events", [])
    )

    html = f"""<!doctype html><html lang="es"><head><meta charset="utf-8">
<title>Reporte - {scenario.name}</title>
<style>
 body{{font-family:sans-serif;margin:24px}}
 table{{border-collapse:collapse;font-size:12px}}
 th,td{{border:1px solid #bbb;padding:3px 8px}}
 h1,h2,h3{{color:#333}}
 .ok{{color:#176b35}} .warn{{color:#9a5b00}}
</style></head><body>
<h1>Reporte de experimento: {scenario.name}</h1>
<p>Generado {datetime.now(timezone.utc).astimezone().isoformat()}</p>
<h2>Parámetros</h2>
<pre>{json.dumps(summary.get('config', {}), indent=2, default=str)}</pre>
<h2>Resultados de tolerancia a fallos</h2>
<table>
<tr><th>Métrico</th><th>Valor</th></tr>
<tr><td>MTTD (detección)</td><td>{analysis.get('mttd_s')} s</td></tr>
<tr><td>MTTR (recuperación)</td><td>{analysis.get('mttr_s')} s</td></tr>
<tr><td>Pico under-replicated</td><td>{analysis.get('peak_underreplicated')}</td></tr>
<tr><td>Pico offline partitions</td><td>{analysis.get('peak_offline')}</td></tr>
<tr><td>Mensajes producidos</td><td>{summary.get('produced')}</td></tr>
<tr><td>Mensajes consumidos</td><td>{summary.get('consumed')}</td></tr>
<tr><td>Mensajes duplicados</td><td>{summary.get('duplicated')}</td></tr>
<tr><td>Pérdida estimada</td><td>{summary.get('message_loss')}</td></tr>
<tr><td>IDs confirmados</td><td>{summary.get('produced_ids')}</td></tr>
<tr><td>IDs procesados</td><td>{summary.get('consumed_ids')}</td></tr>
<tr><td>Errores Prometheus</td><td>{len(summary.get('prometheus_errors', []))}</td></tr>
</table>
<h2>Resumen por fase</h2>
<table><tr><th>Fase</th><th>Muestras</th><th>Lag medio</th><th>Lag pico</th>
<th>Lag p95</th><th>Under-replicated pico</th><th>Offline pico</th><th>Controller mínimo</th></tr>
{phase_rows}</table>
<h2>Eventos reales</h2>
<table><tr><th>Tipo</th><th>Fallo</th><th>t (s)</th><th>Broker</th></tr>
{event_rows}</table>
<h2>Gráficos</h2>
"""
    for name, b64 in charts.items():
        html += f'<h3>{name}</h3><img src="data:image/png;base64,{b64}"><br>'
    html += f"""<h2>Muestras (muestreo cada 50)</h2>
<table><tr><th>t(s)</th><th>fase</th><th>underrep</th><th>offline</th>
<th>ctrl</th><th>lag</th></tr>
{metrics_tbl}
</table>
"""
    for f in scenario.faults:
        html += f"<p>Fault: {f.kind} @ {f.at}s broker={f.broker} down_for={f.down_for}s</p>"
    html += "</body></html>"
    out.write_text(html, encoding="utf-8")


# --------------------------------------------------------------------------
# Orquestación principal
# --------------------------------------------------------------------------
def run_producer(bootstrap, topic, rate, run_id: str,
                 output_path: Path, ids_path: Path) -> subprocess.Popen:
    output = output_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [sys.executable, str(APP_DIR / "order-producer.py"),
         "--bootstrap", bootstrap, "--rate", str(rate), "--topic", topic,
         "--run-id", run_id, "--ids-file", str(ids_path)],
        stdout=output, stderr=subprocess.STDOUT, text=True,
    )
    process.output_file = output
    return process


def run_consumer(bootstrap, topic, group, run_id: str,
                 output_path: Path, ids_path: Path) -> subprocess.Popen:
    output = output_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [sys.executable, str(APP_DIR / "order-consumer.py"),
         "--bootstrap", bootstrap, "--group", group, "--topic", topic,
         "--run-id", run_id, "--ids-file", str(ids_path)],
        stdout=output, stderr=subprocess.STDOUT, text=True,
    )
    process.output_file = output
    return process


def read_process_log(proc: subprocess.Popen) -> list[str]:
    output = getattr(proc, "output_file", None)
    if output is None:
        return []
    output.flush()
    output.close()
    return Path(output.name).read_text(encoding="utf-8").splitlines()


def run(scenario: Scenario, out_dir: Path, dry: bool) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    prom = PrometheusClient(scenario.prometheus_url)
    injector = SshFaultInjector(scenario.ssh_key, scenario.ssh_user)
    recorder = MetricsRecorder(prom, out_dir / "raw_series.csv")
    run_id = f"{scenario.name}-{int(time.time())}"

    # Línea base y fases
    last_fault_end = max(
        (f.at + (f.duration if f.kind == "rebalance" else f.down_for)
         for f in scenario.faults),
        default=scenario.baseline_seconds,
    )
    timeline = {
        "baseline_end": scenario.baseline_seconds,
        "recovery_start": last_fault_end,
        "total": last_fault_end + scenario.recovery_seconds,
    }
    summary = {
        "config": {
            "name": scenario.name,
            "bootstrap": scenario.bootstrap,
            "topic": scenario.topic,
            "consumer_group": scenario.consumer_group,
            "producer_rate": scenario.producer_rate,
            "run_id": run_id,
            "faults": [f.__dict__ for f in scenario.faults],
        }
    }

    # Lanzar clientes
    producer = run_producer(
        scenario.bootstrap, scenario.topic, scenario.producer_rate, run_id,
        out_dir / "producer.log", out_dir / "produced_ids.txt",
    )
    consumer = run_consumer(
        scenario.bootstrap, scenario.topic, scenario.consumer_group, run_id,
        out_dir / "consumer.log", out_dir / "consumed_ids.txt",
    )
    consumer_processes = [consumer]
    pending_consumer_stops: list[tuple[float, subprocess.Popen]] = []

    fault_times: dict[int, float] = {}
    pending_restarts: list[tuple[float, Fault]] = []
    events: list[dict[str, Any]] = []
    try:
        start = time.time()
        print(f"[{scenario.name}] iniciando. Línea base {scenario.baseline_seconds}s.")
        while True:
            now = time.time()
            t = now - start

            phase = ("baseline" if t < timeline["baseline_end"]
                     else "fault" if t < timeline["recovery_start"]
                     else "recovery")
            lag = prom.latest("kafka_consumergroup_lag",
                              {"consumergroup": scenario.consumer_group})
            row = recorder.sample(phase, lag)
            if t % 10 < 1.0:
                print(f"  t={t:6.1f}s [{phase}] "
                      f"under={row['underreplicated']:.0f} "
                      f"offline={row['offline']:.0f} "
                      f"ctrl={row['active_controller']:.0f} lag={lag:.0f}",
                      flush=True)

            # Disparar fallos programados
            for fault_index, f in enumerate(scenario.faults):
                if fault_index not in fault_times and t >= f.at:
                    fault_times[fault_index] = now
                    events.append({
                        "type": "fault_started",
                        "fault_index": fault_index,
                        "elapsed_s": round(t, 2),
                        "broker": f.broker,
                        "kind": f.kind,
                    })
                    if not dry:
                        if f.kind == "broker-failure":
                            injector.stop_broker(f.broker)
                            if f.restart and f.down_for > 0:
                                pending_restarts.append((now + f.down_for, f))
                        elif f.kind == "full-restart":
                            hosts = [b["host"] for b in scenario.brokers]
                            injector.stop_all(hosts)
                            if f.restart and f.down_for > 0:
                                pending_restarts.append((now + f.down_for, f))
                        elif f.kind == "rebalance":
                            consumer_index = len(consumer_processes)
                            extra_consumer = run_consumer(
                                scenario.bootstrap, scenario.topic,
                                scenario.consumer_group, run_id,
                                out_dir / f"consumer-{consumer_index}.log",
                                out_dir / f"consumed_ids-{consumer_index}.txt",
                            )
                            consumer_processes.append(extra_consumer)
                            if f.duration > 0:
                                pending_consumer_stops.append(
                                    (now + f.duration, extra_consumer)
                                )
                            print("  [fault] rebalance: consumidor extra iniciado",
                                  flush=True)
                    print(f"  -> fault ejecutado {f.kind} @ t={t:.1f}s", flush=True)

            for restart_at, f in pending_restarts[:]:
                if now >= restart_at:
                    if not dry:
                        if f.kind == "broker-failure":
                            injector.start_broker(f.broker)
                        elif f.kind == "full-restart":
                            injector.start_all([b["host"] for b in scenario.brokers])
                    pending_restarts.remove((restart_at, f))
                    events.append({
                        "type": "fault_recovered",
                        "elapsed_s": round(t, 2),
                        "broker": f.broker,
                        "kind": f.kind,
                    })
                    print(f"  -> recuperación ejecutada {f.kind} @ t={t:.1f}s",
                          flush=True)

            for stop_at, process in pending_consumer_stops[:]:
                if now >= stop_at:
                    process.send_signal(signal.SIGINT)
                    process.wait(timeout=5)
                    pending_consumer_stops.remove((stop_at, process))
                    events.append({
                        "type": "rebalance_consumer_stopped",
                        "elapsed_s": round(t, 2),
                    })
                    print(f"  -> consumidor extra detenido @ t={t:.1f}s", flush=True)

            if t >= timeline["total"]:
                break
            time.sleep(2.0)

    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
    finally:
        # Finalizar clientes
        for p in [producer, *consumer_processes]:
            try:
                p.send_signal(signal.SIGINT)
                p.wait(timeout=5)
            except Exception:  # noqa: BLE001
                p.kill()
        prod_lines = read_process_log(producer)
        cons_lines = []
        for process in consumer_processes:
            cons_lines.extend(read_process_log(process))
        parser = _parse_summaries(prod_lines, cons_lines)
        summary.update(parser)
        produced_ids = _read_ids(out_dir / "produced_ids.txt")
        consumed_ids = set()
        for ids_path in out_dir.glob("consumed_ids*.txt"):
            consumed_ids.update(_read_ids(ids_path))
        summary["produced_ids"] = len(produced_ids)
        summary["consumed_ids"] = len(consumed_ids)
        summary["missing_ids"] = len(produced_ids - consumed_ids)
        summary["unexpected_ids"] = len(consumed_ids - produced_ids)
        summary["message_loss"] = len(produced_ids - consumed_ids)
        summary["prometheus_errors"] = prom.errors[:20]
        summary["events"] = events
        summary["phase_statistics"] = summarize_phases(recorder.rows)
        summary["data_quality"] = {
            "samples": len(recorder.rows),
            "phases": sorted({row["phase"] for row in recorder.rows}),
            "prometheus_error_count": len(prom.errors),
            "producer_exit_code": producer.returncode,
            "consumer_exit_codes": [process.returncode for process in consumer_processes],
        }
        recorder.close()

    # Análisis sobre el primer fault tipo broker-failure (para MTTD/MTTR)
    first_fault_index = next(
        (index for index, f in enumerate(scenario.faults)
         if f.kind == "broker-failure" and index in fault_times),
        None,
    )
    if first_fault_index is not None:
        analysis = compute_mttd_mttr(recorder.rows, fault_times[first_fault_index])
    else:
        analysis = {}

    analysis["faults"] = {
        str(index): compute_mttd_mttr(recorder.rows, fault_times[index])
        for index, fault in enumerate(scenario.faults)
        if fault.kind == "broker-failure" and index in fault_times
    }

    summary["analysis"] = analysis
    (out_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, default=str), encoding="utf-8")

    render_report(scenario, recorder.rows, analysis, summary, out_dir / "report.html")
    print(f"\nResultados en: {out_dir}")
    print(json.dumps(analysis, indent=2))
    return summary


def _parse_summaries(prod_lines: list[str], cons_lines: list[str]) -> dict:
    s = {"produced": None, "failed": None, "consumed": None, "duplicated": None,
         "consumer_errors": None}
    for ln in prod_lines:
        if "RESUMEN productor" in ln:
            s["produced"] = _num(ln, "enviados")
            s["failed"] = _num(ln, "fallidos")
    for ln in cons_lines:
        if "RESUMEN consumidor" in ln:
            s["consumed"] = _num(ln, "procesados")
            s["duplicated"] = _num(ln, "duplicados")
    # Valor preliminar; run() lo reemplaza por la diferencia real de IDs.
    if s["consumed"] is not None and s["produced"] is not None:
        s["message_loss"] = max(0, s["produced"] - s["consumed"])
    else:
        s["message_loss"] = None
    return s


def _read_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()}


def _num(line: str, key: str) -> int:
    import re

    m = re.search(rf"{key}:\s*(\d+)", line)
    return int(m.group(1)) if m else None


# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Runner de experimentos de tolerancia a fallos")
    ap.add_argument("--scenario", required=True, help="ruta al YAML de escenario")
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    ap.add_argument("--dry", action="store_true", help="no ejecutar fallos, solo captura")
    ap.add_argument("--preflight", action="store_true",
                    help="validar Kafka y Prometheus sin iniciar clientes ni fallos")
    ap.add_argument("--ssh-key", help="clave SSH; vacío usa el SSH agent")
    ap.add_argument("--ssh-user", help="usuario SSH para inyectar fallos")
    args = ap.parse_args()

    src = Path(args.scenario)
    cfg = yaml.safe_load(src.read_text())
    scenario = Scenario.from_dict(cfg)
    if args.ssh_key is not None:
        scenario.ssh_key = args.ssh_key
    if args.ssh_user is not None:
        scenario.ssh_user = args.ssh_user
    out_dir = Path(args.out) / scenario.name

    if args.preflight:
        result = run_preflight(scenario)
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "preflight.json").write_text(
            json.dumps(result, indent=2), encoding="utf-8",
        )
        raise SystemExit(0 if result["ok"] else 1)

    run(scenario, out_dir, args.dry)


if __name__ == "__main__":
    main()