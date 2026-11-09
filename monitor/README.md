localización: este directorio replica la estructura de la VM `monitor`.

| Ruta | Archivo | Uso |
|---|---|---|
| `monitor/prometheus/prometheus.yml` | Plantilla de Prometheus | Referencia; la versión con IPs reales la genera Ansible |
| `monitor/grafana/dashboard-fault-tolerance.json` | Dashboard de tolerancia a fallos | Importar en Grafana → Dashboards → Import |
| `monitor/grafana/dashboard-tp2.json` | Dashboard de envío/recepción del TP2 | Importar en Grafana → Dashboards → Import |
| `monitor/exporters/` | Config JMX + units systemd | Desplegadas vía Ansible en brokers y monitor |

### Importar el dashboard de Grafana
Tras abrir los túneles (`scripts/tunnels.sh`):
1. `http://localhost:3000` (admin/admin).
2. **Dashboards → Import** → `monitor/grafana/dashboard-fault-tolerance.json`.
3. Seleccionar el datasource `Prometheus`.

Para el TP2, importe `monitor/grafana/dashboard-tp2.json` (UID
`kafka-tp2-envio-recepcion`, ruta `/d/kafka-tp2-envio-recepcion/`). Ese
dashboard muestra el envío (`messagesin_total`, `bytesin_total`, log end
offset) y la recepción (`bytesout_total`). El TP2 usa
`enable.auto.commit=false`, por lo que `kafka_consumergroup_lag` y
`kafka_consumergroup_current_offset` quedan vacíos; la recepción se prueba con
el check V5 y `kafka_log.jsonl`/`DELIVERED_DIGEST`. Use `TP2_KEEP_TOPIC=1`
para conservar el tópico durante la inspección.

### Métricas Clave

| Métrica prometheus | Significado |
|---|---|
| `kafka_server_replicamanager_underreplicatedpartitions` | Réplicas desincronizadas (0 = sano) |
| `kafka_controller_kafkacontroller_offlinepartitionscount` | Particiones sin líder (0 = sano) |
| `kafka_controller_kafkacontroller_activecontrollercount` | 1 = hay controlador |
| `kafka_consumergroup_lag` | Lag del consumidor |
| `kafka_server_replicamanager_isrshrinks_total` | Contracciones de ISR (indican fallos) |
| `kafka_server_brokertopicmetrics_messagesin_total` | Throughput |

Estas mismas métricas son las que consulta `experiments/experiment_runner.py` vía la
API de Prometheus (`/api/v1/query`) para calcular MTTD/MTTR y generar el reporte.