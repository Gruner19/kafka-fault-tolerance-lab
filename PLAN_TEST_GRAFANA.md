# Plan de Test para Grafana + Prometheus + Kafka (TP01)

## 1. Acceso (túnel SSH → localhost)

```bash
# Terminal 1: Grafana
ssh -i ~/Descargas/SSH/kafka-cluster-key.pem \
  -L 3000:localhost:3000 ubuntu@18.220.154.71

# Terminal 2: Prometheus (opcional, para consultas directas)
ssh -i ~/Descargas/SSH/kafka-cluster-key.pem \
  -L 9090:localhost:9090 ubuntu@18.220.154.71
```
- URL Grafana: `http://localhost:3000`  
- Login: `admin` / `admin`

## 2. Importar dashboard del proyecto

En Grafana:
1. Dashboards → Import
2. Cargar archivo: `monitor/grafana/dashboard-fault-tolerance.json`
3. Datasource: `Prometheus`
4. El dashboard muestra 6 paneles: under-replicated, offline partitions, active controller, consumer lag, throughput, ISR shrinks.

## 3. Verificar que Prometheus está scrapeando (en Grafana)

- Abre el panel **Active controller** (`kafka_controller_kafkacontroller_activecontrollercount`). Debe valer `1` siempre.
- Si muestra `No data found`: revisa `Status → Targets` (botón `Targets` en Prometheus, o consulta `http://localhost:9090/targets`). Todos los targets deben ser `UP`.

## 4. Generar carga real (topic `orders`)

### 4.1 Crear el topic (si no existe)

```bash
ssh -i ~/Descargas/SSH/kafka-cluster-key.pem ubuntu@18.225.31.122 \
  'sudo -u kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic orders --bootstrap-server 172.31.37.6:9092 \
  --partitions 3 --replication-factor 3'
```

### 4.2 Lanzar productor (en una VM o en tu máquina con túnel)

```bash
cd scripts/app
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python3 order-producer.py \
  --bootstrap 172.31.37.6:9092 \
  --rate 5 \
  --duration 120 \
  --topic orders
```

Resultado esperado: imprime líneas como `✓ orders[0] offset 42 ...`. Exit 0 si no hay errores.

### 4.3 Lanzar consumidor (grupo `order-processor-group`)

```bash
python3 order-consumer.py \
  --bootstrap 172.31.37.6:9092 \
  --group order-processor-group \
  --topic orders \
  --duration 120
```

## 5. Observar en Grafana con carga activa

Con productor + consumidor corriendo:

- **Throughput mensajes** (`kafka_server_brokertopicmetrics_messagesin_total`): debe subir a > 5 msg/s.
- **Consumer lag** (`kafka_consumergroup_lag`): debe mantenerse bajo si el consumidor sigue el ritmo. Si sube, hay cuello de botella.
- **ISR shrinks** (`kafka_server_replicamanager_isrshrinks_total`): debe ser `0` en condiciones normales.
- **Under-replicated partitions**: debe ser `0`.

## 6. Escenario de tolerancia a fallos (opcional / experimento automático)

### 6.1 Manual (simula `broker-failure.yaml`)

Con carga activa (paso 4):

```bash
# Desde tu máquina (o via SSH a monitor):
ssh -i ~/Descargas/SSH/kafka-cluster-key.pem ubuntu@3.22.234.84 \
  'sudo systemctl stop kafka'

# Espera 45 segundos (según escenario)
# Observa en Grafana: active controller debe seguir en 1,
# under-replicated puede subir brevemente y luego bajar.

# Reinicia
ssh -i ~/Descargas/SSH/kafka-cluster-key.pem ubuntu@3.22.234.84 \
  'sudo systemctl start kafka'
```

### 6.2 Automático (script de experimentos)

```bash
# Requiere túnel Prometheus en localhost:9090
python3 experiments/experiment_runner.py \
  --scenario experiments/scenarios/broker-failure.yaml \
  --duration 150
```

Esto ejecuta: baseline 60s → caída broker-2 a los 60s (45s abajo) → recuperación 90s. Al finalizar genera un reporte con MTTD y MTTR consultado vía Prometheus (`http://localhost:9090/api/v1/query`).

## 7. Verificación final (checklist)

- [ ] Grafana accesible (`http://localhost:3000`) con `admin/admin`
- [ ] Dashboard `Kafka Tolerancia a Fallos (TP01)` importado
- [ ] Panel **Active controller** muestra `1`
- [ ] Panel **Consumer lag** muestra datos (si hay consumidor corriendo)
- [ ] Panel **Throughput mensajes** sube tras lanzar `order-producer.py`
- [ ] Experimento `broker-failure.yaml` no genera errores críticos (exit 0 del runner)
- [ ] `node_exporter` (target `localhost:9101`) está `UP` después de aplicar el role monitoring

## Notas rápidas

- Si `Prometheus → Targets` muestra algún broker como `DOWN`, revisa `/etc/hosts` en los brokers (`/etc/hosts` debe contener `broker_ip` → `kafka-1/2/3`) y que los puertos `9100` (JMX exporter) estén abiertos entre brokers y monitor.
- El datasource `Prometheus` está configurado automáticamente por el playbook (`monitoring/templates/grafana-datasources.yml.j2`) apuntando a `http://localhost:9090`.
- El dashboard JSON usa `datasource: 1`; al importar debe seleccionar el datasource existente (`Prometheus`).
