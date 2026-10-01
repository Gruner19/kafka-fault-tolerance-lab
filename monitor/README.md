localización: este directorio replica la estructura de la VM `monitor`.

| Ruta | Archivo | Uso |
|---|---|---|
| `monitor/prometheus/prometheus.yml` | Plantilla de Prometheus | Referencia; la versión con IPs reales la genera Ansible |
| `monitor/grafana/dashboard-fault-tolerance.json` | Dashboard de tolerancia a fallos | Importar en Grafana → Dashboards → Import |
| `monitor/exporters/` | Config JMX + units systemd | Desplegadas vía Ansible en brokers y monitor |

### Importar el dashboard de Grafana
Tras abrir los túneles (`scripts/tunnels.sh`):
1. `http://localhost:3000` (admin/admin).
2. **Dashboards → Import** → `monitor/grafana/dashboard-fault-tolerance.json`.
3. Seleccionar el datasource `Prometheus`.

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