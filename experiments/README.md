# Runner de experimentos de tolerancia a fallos

Captura métricas en tiempo real (vía Prometheus), inyecta fallos (vía SSH)
y genera análisis + reporte para documentar el comportamiento del clúster.

## Flujo
```
túnel (scripts/tunnels.sh)         → prometheus_url localhost:9090
python3 experiment_runner.py --scenario scenarios/broker-failure.yaml
        │
        ├─ lanza order-producer.py + order-consumer.py
        ├─ muestrea métricas cada 2s (línea base → fallo → recuperación)
        ├─ inyecta fallos programados (systemctl stop/start vía SSH)
        └─ calcula MTTD/MTTR y genera:
              output/<escenario>/raw_series.csv
              output/<escenario>/summary.json
              output/<escenario>/report.html   (+ gráficos PNG embebidos)
              output/<escenario>/{producer,consumer}.log
              output/<escenario>/*_ids*.txt
```

## Escenarios incluidos
| Archivo | Qué prueba |
|---|---|
| `broker-single-kafka*.yaml` | Caída individual de cada broker |
| `broker-pair-*.yaml` | Caída simultánea de cada pareja de brokers |
| `full-restart.yaml` | Reinicio total del clúster (durabilidad en disco) |
| `rebalance.yaml` | Cuatro rebalanceos de consumer group bajo 30 msg/s |

`broker-failure.yaml` se conserva como escenario histórico, pero no debe usarse
para resultados comparables porque mezcla fallos individuales, pares y triple
caída dentro de una misma línea temporal.

## Uso
```bash
# dependencias
pip install -r ../scripts/app/requirements.txt

# diagnóstico seguro antes de cualquier experimento
python3 experiment_runner.py --scenario scenarios/full-restart.yaml --preflight

# con túneles activos
python3 experiment_runner.py --scenario scenarios/broker-failure.yaml
python3 experiment_runner.py --scenario scenarios/broker-failure.yaml --dry  # sin disparar fallos
```

## Ejecución recomendada desde el monitor

Los brokers anuncian sus IPs privadas. Para evitar que el cliente local intente
conectarse directamente a esas direcciones, se puede ejecutar todo el
experimento dentro de la VM `monitor`:

```bash
ssh-add /home/gruner-sanchez/Descargas/SSH/kafka-cluster-key.pem
./scripts/run_remote_experiment.sh 18.220.154.71 full-restart.yaml
```

El launcher usa SSH agent forwarding, instala las dependencias en un entorno
temporal del monitor, usa `kafka-1:9092` como bootstrap y copia el resultado a
`output/remote-<timestamp>/`. No copia la clave privada al monitor.

El preflight no inicia clientes ni detiene brokers. Comprueba Prometheus, sus
targets, el controlador activo, réplicas, particiones offline y la metadata del
topic. Guarda el resultado en `output/<escenario>/preflight.json` y devuelve
código distinto de cero si alguna condición crítica falla.

## Métricas que captura (`PrometheusClient.METRICS`)
- `underreplicated` — particiones sub-replicadas (clave de tolerancia)
- `offline` — particiones sin líder
- `active_controller` — 1 = hay controlador
- `isr_shrinks` / `isr_grows` — dinámica del ISR
- `messages_in` — throughput
- `consumer_lag` — desde kafka_exporter

## Identidad y análisis generado

Cada ejecución genera un `run_id`. El productor añade ese identificador a cada
pedido y guarda los IDs confirmados; el consumidor procesa solo ese `run_id` y
guarda los IDs procesados. Así, `message_loss` se calcula como la diferencia
entre conjuntos de IDs, no como una resta de contadores afectados por backlog.

- **MTTD** (time-to-detect): primer instante con `underreplicated > 0` tras el fallo.
- **MTTR** (time-to-recover): primera vuelta a `underreplicated = 0`.
- **Pérdida de mensajes**: IDs confirmados por Kafka que no aparecen procesados.
- **Errores de Prometheus**: se conservan en `summary.json`; no se convierten silenciosamente en muestras válidas.

El `summary.json` también incluye:

- `phase_statistics`: muestras, lag medio, lag pico, lag p95 y picos de salud por fase.
- `events`: timestamps reales de fallos, recuperaciones y detenciones de consumidores.
- `data_quality`: cantidad de muestras, fases observadas, errores de Prometheus y códigos de salida.
- `missing_ids` y `unexpected_ids`: diferencias entre IDs confirmados y procesados.

El `report.html` presenta estos datos en tablas de resumen por fase y eventos,
además de los gráficos originales.

El dashboard Grafana añade variables para `topic`, `consumer_group` y `broker`.
También muestra bytes de entrada del cluster, disponibilidad de brokers, CPU y memoria del monitor.
El lag y el throughput se filtran por topic/grupo y usan ventanas de 30 segundos
para hacer visibles fallos breves.

> Importante: el runner asume `bootstrap=kafka-1:9092` local vía túnel (el puerto
> 9092 se reenvía a un broker). Ajusta `bootstrap` si los clientes corren en otro lado.

El escenario `rebalance.yaml` inicia un consumidor adicional en el mismo group y
lo detiene después de `duration`, provocando un rebalanceo real. Cada proceso
escribe su propio log y archivo de IDs para poder consolidar el resultado.