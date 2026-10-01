# PROYECTO_CONTEXTO.md — Sistema de Monitoreo de Tolerancia a Fallos de Kafka

> Documento de contexto vivo para retomar el proyecto rápidamente y para que
> el asistente (opencode) tenga el contexto completo ante futuras consultas.
> **Última actualización:** 2026-09-18

---

## 1. Objetivo y alcance

Construir, desplegar y probar la **tolerancia a fallos de un clúster de Apache Kafka
de 3 nodos (modo KRaft)**, sobre **instancias AWS EC2**, con un stack de monitoreo
(Prometheus + Grafana + exporters) accesible desde la máquina local mediante
**túneles SSH**.

El TP requiere **capturar los resultados** de los experimentos (MTTD/MTTR, lag,
pérdida de mensajes) y **mostrarlos para análisis** (reportes HTML/CSV y dashboards).

**Decisiones tomadas con el usuario (preguntas iniciales):**
- Sistema: stack completo (Prometheus + Grafana + exporters).
- Acceso desde local: túneles/SSH remoto hacia la VM `monitor` de EC2.
- Despliegue: Terraform para infraestructura + Ansible para configuración.
- Estado actual: el usuario **ya tiene las 4 VMs creadas en AWS** → NO usa
  Terraform para crear; solo Ansible apuntando al inventario.

## 2. Arquitectura

```
TU MÁQUINA LOCAL (analista)
      │  túneles SSH (scripts/tunnels.sh)
      ▼               AWS / EC2
┌──────────────────────────────────────────────────────────┐
│  ┌────────┐ ┌────────┐ ┌────────┐        ┌──────────┐   │
│  │ kafka-1 │ │ kafka-2 │ │ kafka-3 │        │ monitor   │   │
│  │ node.id1│ │ node.id2│ │ node.id3│        │ Prom+Gra |  │
│  │ 9092/9093│ │ 9092/9093│ │ 9092/9093│        │ 9090/3000 │   │
│  │ jmx 9100│ │ jmx 9100│ │ jmx 9100│        │ exp 9308 │   │
│  └────────┘ └────────┘ └────────┘        └──────────┘   │
└──────────────────────────────────────────────────────────┘
```

| VM | Rol | Puertos | Stack |
|---|---|---|---|
| kafka-1/2/3 | Broker + Controller (KRaft) | 9092 cliente, 9093 quórum, 9100 JMX | Java 17, Kafka 3.7.2, JMX agent |
| monitor | Monitoreo | 9090 Prom, 3000 Grafana, 9308 kafka_exporter | Prometheus 2.48, Grafana, kafka_exporter 1.7.0 |

### Túneles SSH (scripts/tunnels.sh)
Reenvíos a la VM monitor:
- `3000` → Grafana
- `9090` → Prometheus (API /api/v1/query)
- `9308` → kafka_exporter (lag)
- `9092` → broker kafka-1 (para clientes locales) — resuelto por el monitor vía /etc/hosts

## 3. Estructura del repositorio

```
.
├── README.md
├── PROYECTO_CONTEXTO.md          ← este documento
├── terraform/                    # [OPCIONAL] para SI las VMs NO existen aún
│   ├── main.tf, variables.tf, outputs.tf, versions.tf
│   ├── render_inventory.sh       # genera ansible/inventories/hosts.yml
│   └── cloud-init/{kafka-node,monitor-node}.sh
├── ansible/                      # ← VÍA PRINCIPAL para VMs existentes
│   ├── site.yml
│   ├── inventories/hosts.yml     # ← VINCULA LAS VMs AL SISTEMA (editar aquí)
│   └── roles/
│       ├── broker/               # instala+configura Kafka y KRaft
│       └── monitoring/           # instala+configura Prometheus/Grafana/exporters
├── monitor/                      # referencias + dashboard Grafana importable
│   ├── prometheus/prometheus.yml.reference
│   ├── grafana/dashboard-fault-tolerance.json
│   └── exporters/
├── scripts/
│   ├── tunnels.sh                # abre túneles SSH
│   └── app/{order-producer,order-consumer}.py
├── experiments/
│   ├── experiment_runner.py      # captura métricas, inyecta fallos, genera reporte
│   ├── scenarios/{broker-failure,full-restart,rebalance}.yaml
│   └── README.md
└── output/                       # resultados generados (por escenario)
```

## 4. Inventario: cómo se vinculan las VMs (CLAVE)

`ansible/inventories/hosts.yml` es el único archivo que hay que tocar para
usar VMs existentes. Formato:

```yaml
all:
  vars:
    ansible_user: ubuntu
    ansible_ssh_private_key_file: "{{ lookup('env','SSH_PRIVATE_KEY') | default('/home/gruner-sanchez/Descargas/SSH/kafka-cluster-key.pem', true) }}"
    kafka_cluster_id: "MI_CLUSTER_ID"     # MISMO en las 3 VMs
    kafka_version: "3.7.2"
    kafka_dist: "2.13"
  children:
    brokers:
      hosts:
        kafka-1:
          ansible_host: "<IP_PUBLICA>"    # SSH desde local
          broker_ip: "<IP_PRIVADA>"       # comunicación interna (opcional)
          node_id: 1
        # kafka-2 (node_id 2), kafka-3 (node_id 3) igual
    monitor:
      hosts:
        monitor:
          ansible_host: "<IP_PUBLICA_MONITOR>"
          broker_ip: "<IP_PRIVADA>"
```

**Reglas:**
- `ansible_host` = IP pública (para que Ansible/el túnel se conecten).
- `broker_ip` = IP privada (para que los brokers se anuncien/alcancen). Si se
  omite, cae a `ansible_host` (útil si todo es en la misma red y solo hay públicas).
- El rol `broker` usa `broker_ip` en `advertised.listeners` y en `/etc/hosts`.
- `kafka_cluster_id` debe ser idéntico en los 3 brokers (generar:
  `/opt/kafka/bin/kafka-storage.sh random-uuid`). Default usado: `MkU3OEVBNTcwNTJENDM2Qk`.

## 5. Flujo de ejecución (VMs existentes)

```bash
# 1) clave privada
export SSH_PRIVATE_KEY=~/.ssh/mi-clave.pem

# 2) configurar todo (instala+configura, idempotente)
cd ansible
ansible-playbook -i inventories/hosts.yml site.yml

# 3) túneles (dejar corriendo)
./scripts/tunnels.sh <IP_PUBLICA_MONITOR>

# 4) crear topic (1 vez)
/opt/kafka/bin/kafka-topics.sh --create --topic orders \
  --bootstrap-server kafka-1:9092 --partitions 3 --replication-factor 3

# 5) experimento
cd ../experiments
python3 experiment_runner.py --scenario scenarios/broker-failure.yaml
# variantes: --dry (no dispara fallos), --out <dir>
```

Reportes generados en `output/<escenario>/`: `report.html`, `raw_series.csv`, `summary.json`.

## 6. Experimentos y métricas

### Escenarios (`experiments/scenarios/`)
| Escenario | Qué prueba |
|---|---|
| `broker-failure` | Caída abrupta del broker 2 → MTTD/MTTR, pérdida, reelección |
| `full-restart` | Reinicio total → durabilidad en disco |
| `rebalance` | Rebalanceo de consumer group |

Cada YAML define: topic, bootstrap, `producer_rate`, `baseline_seconds`,
`recovery_seconds`, `consumer_group`, brokers, `faults` (tipo, `at`, `broker`,
`down_for`, `restart`). Tipos de fault: `broker-failure`, `full-restart`,
`rebalance`.

### Métricas capturadas vía Prometheus API (`PrometheusClient.METRICS`)
| Clave | Métrica prometheus | Significado |
|---|---|---|
| underreplicated | `kafka_server_replicamanager_underreplicatedpartitions` | réplicas desincronizadas (0=sano) |
| offline | `kafka_controller_kafkacontroller_offlinepartitionscount` | particiones sin líder (0=sano) |
| active_controller | `kafka_controller_kafkacontroller_activecontrollercount` | 1=hay controlador |
| isr_shrinks / grows | `kafka_server_replicamanager_isr_{shrinks,grows}_total` | dinámica ISR |
| messages_in | `kafka_server_brokertopicmetrics_messagesin_total` | throughput |
| consumer_lag | `kafka_consumergroup_lag` (kafka_exporter) | retraso del consumidor |

### Análisis (`compute_mttd_mttr`)
- **MTTD**: primer instante con `underreplicated > 0` tras el fallo.
- **MTTR**: la **primera** vuelta a `underreplicated = 0` (se corrigió un bug:
  antes se sobreescribía con la última muestra; ahora hace `break` en la primera).
- **Pérdida de mensajes**: `producidos − consumidos` (parsed de los resúmenes).
- Exit code / resumen: productor y consumidor imprimen `RESUMEN ...` parseado por `_parse_summaries`.

## 7. Configuración de tolerancia (KRaft) aplicada

En cada `server-node<N>.properties`: `replication-factor=3`,
`min.insync.replicas=2`, `process.roles=broker,controller`,
`controller.quorum.voters=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093`. Esto
garantiza tolerar la caída de 1 broker sin pérdida de datos.

## 8. Requisitos de red en AWS (SG) — caveat técnico

Sin esto el clúster NO forma quórum aunque `/etc/hosts` esté bien:
- `9092` y `9093` **entre los 3 brokers**.
- `9100` (JMX) del monitor → brokers.
- `9090`, `3000`, `9308` (acceso directo) o solo `22` si se usan túneles SSH.

## 9. Estado y temas pendientes

- [x] Terraform: infraestructura EC2 (crear 4 instancias + SG + cloud-init).
- [x] Ansible: rol broker (instalación idempotente + KRaft + systemd + JMX).
- [x] Ansible: rol monitoring (instalación + prometheus.yml con IPs reales + Grafana datasource).
- [x] Túneles SSH + apps productor/consumidor.
- [x] Runner de experimentos (captura + MTTD/MTTR + CSV/JSON/HTML).
- [x] Dashboard Grafana exportable + READMEs.
- [ ] El usuario debe rellenar `ansible/inventories/hosts.yml` con sus IPs reales.
- [ ] Ajustar security groups de AWS para los puertos internos.
- [ ] No validado con `terraform plan` (Terraform no instalado localmente).
- [ ] `experiments/runner` prueba `rebalance` aún simple: solo muestra la intención.

## 10. Comandos útiles de verificación

```bash
# estado del quórum
/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status
# réplicas (tolerancia)
/opt/kafka/bin/kafka-topics.sh --describe --topic orders --bootstrap-server kafka-1:9092
# logs de un broker
sudo journalctl -u kafka -f
# prometheus targets (por túnel)
curl -H 'content-type: application/json' --data '{"/api/v1/targets":[]}' http://localhost:9090/-/ready
```