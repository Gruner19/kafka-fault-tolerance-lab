# Plan de ejecución TP2 — B6 sobre Kafka

> Documento interno de trabajo (español). Los entregables del TP (`tp2/README.md`,
> comentarios, logs legibles) van en **pt-BR** según la spec.

Referencia: `SPEC_TP2_AGENTE_IA.md` (v. 2026-11-04).

---

## 1. Análisis: para qué sirve (y para qué NO) el MCP de Kafka

MCP instalado: `kafka-mcp-server v2.0.2` (binario en `~/.local/bin/kafka-mcp-server`),
registrado global en OpenCode como `kafka`, con
`KAFKA_BROKERS=localhost:9092`, `KAFKA_CLIENT_ID=kafka-mcp-server`, transporte stdio.

### 1.1 Herramientas disponibles y su valor real

| Herramienta MCP | Uso en el TP2 | Fase |
|---|---|---|
| `list_brokers` | Comprobar que el Kafka de desarrollo (docker-compose) responde. | 1–5 |
| `cluster_overview` | Preflight: brokers, controlador, particiones sub-replicadas, offline. Equivale a parte de `--preflight` del TP1. | 1, 4–5 |
| `list_topics` | Verificar que `r-events-<run>` existe y que `partition_count == 1` (trampa de `num.partitions=3`). | 1, 4–5 |
| `describe_topic` | Ver líder, réplicas e ISR de la partición 0; detectar topics auto-creados con 3 particiones. | 1, 3–5 |
| `describe_configs` (topic) | Verificar `min.insync.replicas`, `retention.ms` y `cleanup.policy=delete` (prohibido compactar). | 1, 4–5 |
| `produce_message` | Smoke test del payload JSON (`ACQUIRE`/`RELEASE`, key `"R"`). Publica síncrono (`ProduceSync`). | 1, 3 |
| `consume_messages` | Inspección cualitativa de los últimos registros (esquema, offsets crecientes, orden). | 1, 3 |
| `list_consumer_groups` | Confirmar que existen los N grupos `sync-<i>-<run>` (uno por proceso). | 3–5 |
| `describe_consumer_group` | Confirmar 1 miembro por grupo asignado a la partición 0 (readiness). | 3–5 |

### 1.2 Limitaciones que condicionan el diseño

1. **No crea ni borra topics ni fija particiones/configs.** El harness debe usar
   `confluent-kafka` `AdminClient` (o `kafka-topics.sh` en dev). El MCP solo verifica.
2. **`produce_message` no acepta partición ni devuelve offset.** Con topic de 1
   partición el destino es trivial, pero el `ACQUIRE` real necesita el offset del
   delivery report → el productor del protocolo es propio, no el MCP.
3. **`consume_messages` no expone `group.id`, offset ni `seek`.** Usa el grupo
   fijo `KAFKA_CLIENT_ID` y hace un único `PollFetches`. Verificado en dev: con
   un grupo nuevo franz-go resetea al inicio (leyó desde offset 0), pero al no
   poder fijar offsets ni paginar de forma determinista **no sirve** para volcar
   el log completo (`kafka_log.jsonl`) ni para V5.
4. **No sirve para medir latencia ni para el camino caliente** (5–10 procesos,
   lecturas por mensaje). El MCP nunca debe estar dentro del experimento.
5. **No hay acceso al clúster AWS desde el host**: Kafka anuncia IPs privadas
   (`advertised.listeners` con `broker_ip`). El MCP es **solo para el Kafka de
   desarrollo**. El smoke real se hace con `preflight` remoto en `monitor`.
6. **Cuidado con el grupo `kafka-mcp-server`**: si consume `r-events-*` durante una
   corrida, añade un consumidor extra que no forma parte del protocolo y contamina
   el conteo de entregas (2N+2). Regla: **no consumir el topic medido mientras corre
   un experimento**.

### 1.3 Reglas de uso

- MCP = **desarrollo y verificación de configuración**, no protocolo ni evidencia.
- Antes de una corrida: `cluster_overview` + `describe_topic` + `describe_configs`.
- Después de una corrida: `list_consumer_groups` / `describe_consumer_group` y,
  como máximo, `consume_messages` sobre las últimas entradas (nunca como evidencia).
- La evidencia sale de `kafka_log.jsonl` generado por el harness (consumer propio
  con `assign()` + `seek(0)`).

---

## 2. Fase 0 — Reconocimiento (resultado de los "VERIFICAR")

| Qué | Resultado |
|---|---|
| Cliente Kafka | `scripts/app/requirements.txt`: `confluent-kafka>=2.3.0`; instalado **2.15.1**. Soporta `enable.idempotence=true` → no hace falta el camino `kafka-python`. |
| Python local | **3.14.4** (venv del repo). |
| Python en `monitor` | **Pendiente**: Ubuntu 22.04 ⇒ probable 3.10. Comprobar por SSH. Escribir código compatible con 3.10 (sin `match`, sin PEP 695, `from __future__ import annotations`). |
| `kafka-1` → IP privada en `monitor` | `ansible/roles/monitoring/tasks/main.yml` línea 70: el rol escribe los brokers en `/etc/hosts` del monitor. Los clientes corren allí. |
| Patrón de ejecución remota | `scripts/run_remote_experiment.sh`: copia con `scp`, crea venv, `sed` de `bootstrap`, ejecuta y trae `output/`. Reutilizar tal cual. |
| Convenciones de salida | `output/<escenario>/{summary.json, raw_series.csv, report.html, preflight.json}` en `experiments/experiment_runner.py`. Mantener estilo en `tp2/output/`. |
| Security Groups | `terraform/main.tf`: 9092/9093 abiertos, pero `advertised.listeners` usa IP privada ⇒ no alcanzable desde el host. |
| Docker | `docker 29.1.3`, `docker compose 2.40.3`. Suficiente para el Kafka de dev. |

Decisiones iniciales a registrar en `tp2/DECISIONES.md`:
- Cliente: `confluent-kafka` con idempotencia.
- Kafka dev: 1 broker KRaft (`apache/kafka:3.7.2`), **RF=1**, `num.partitions=3` y
  `auto.create.topics.enable=false` para reproducir la trampa del TP1.
- Topic por corrida: `r-events-<run_id>`, 1 partición, retención 24 h.
- MCP solo dev; AWS vía preflight remoto.

**Aceptación:** tabla anterior completa (solo queda el Python remoto, que se valida
en el preflight de la fase 6).

---

## 3. Plan por fases

### Fase 1 — Entorno de desarrollo + MCP operativo

Entregables:
```
tp2/docker/Dockerfile              # opcional (imagen propia) 
tp2/docker/docker-compose.dev.yml  # Kafka KRaft 1 nodo, localhost:9092
tp2/requirements.txt               # versiones == (confluent-kafka, PyYAML, pytest)
tp2/src/harness/admin.py           # crear/borrar/verificar topic (AdminClient)
```

Tareas:
1. `docker compose -f tp2/docker/docker-compose.dev.yml up -d` con:
   - `KAFKA_NODE_ID=1`, rol `broker,controller`, `PLAINTEXT://:9092`,
     `ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092`.
   - `num.partitions=3`, `default.replication.factor=1`, `auto.create.topics.enable=false`.
2. Crear venv `tp2/.venv` e instalar `requirements.txt` con versiones fijadas.
3. `admin.py create --topic r-events-smoke --partitions 1 --rf 1 --retention-ms 86400000`.
4. Smoke con Python (produce key `R`, consume con `assign`+`seek(0)`).
5. Smoke con el MCP (esto valida el MCP y el entorno):
   - `list_brokers` → `localhost:9092`
   - `cluster_overview` → 1 broker, 0 offline, 0 sub-replicadas
   - `list_topics` → `r-events-smoke` con `partition_count=1`
   - `describe_topic` → solo `partitionID 0`
   - `describe_configs` → `retention.ms=86400000`, `cleanup.policy=delete`
   - `produce_message` (`key="R"`, `value={"type":"ACQUIRE",...}`)
   - `consume_messages` → leer el registro y comprobar offset/JSON

**Aceptación:** 1 mensaje producido y consumido en la partición 0, verificado por
Python **y** por el MCP.

---

### Fase 2 — Núcleo del algoritmo (`kafka_sync.py`) sin Kafka

Entregables:
```
tp2/src/common/{messages.py, jsonl_log.py, clock.py, config.py}
tp2/src/sync_node/kafka_sync.py    # QueueState puro + adaptador Kafka
tp2/tests/unit/test_queue_state.py
tp2/tests/sim/test_simulation.py
```

Tareas:
1. `QueueState`: `on_deliver`/`acquire`/`release` según pseudocódigo 3.3, con
   `queue`, `seen`, `Condition`, dedupe por `request_id`, `INVARIANT_VIOLATION`.
2. Separar la regla (pura, testeable) del I/O de Kafka.
3. Unitarias: duplicados, `RELEASE` fuera de orden, cola vacía, `RELEASE` de un
   `request_id` que no es cabeza.
4. Simulación: log falso ordenado + 5 nodos con ritmos aleatorios, ≥1000 semillas;
   afirmar exclusión mutua, FIFO por offset y cero violaciones.

**Aceptación:** ≥1000 semillas sin violaciones; cobertura de los 4 casos unitarios.
MCP: no interviene (no hay Kafka).

---

### Fase 3 — Nodo, cliente, observador y logs

Entregables:
```
tp2/src/sync_node/{node.py, critical_section.py, client_server.py}
tp2/src/client/client.py
tp2/src/observer/observer.py
```

Tareas:
1. TCP/JSONL cliente↔nodo (`REQUEST`/`COMMITTED`), puertos `7001+i`.
2. Worker único por nodo (cola FIFO local, sección 3.4).
3. Orden estricto 4.3: `REQUEST → acquire → ENTER → BEGIN(+ACK) → sleep
   U(0.2,1.0) → END(+ACK) → EXIT → release → COMMITTED`.
4. Observador con `active`, `capacity`, `OVERLAP`, `max_active`, secuencia de llegada;
   ACK tras registrar.
5. Logs JSONL por actor con `mono_ns`, `offset`, `queue_head`, `queue_len`, `run_id`.
6. Consumer: grupo `sync-<i>-<run>`, `enable.auto.commit=false`, `auto.offset.reset=earliest`,
   un hilo, `READY` tras asignación de la partición 0.

Aceptación: corrida local 3 nodos / 3 clientes / 5 pedidos; se verifica el orden
4.3 y offsets ascendentes en `node-*.jsonl`.
MCP (solo después de cerrar los procesos): `list_consumer_groups` muestra los 3
grupos; `describe_consumer_group` muestra 1 miembro por grupo en la partición 0.

---

### Fase 4 — Harness, verificador y modo control

Entregables:
```
tp2/src/harness/{run_experiment.py, verify_run.py}
tp2/src/harness/scenarios/*.yaml
```

Tareas:
1. `run_experiment.py`: crear topic (1 partición; RF=1 dev / RF=3 AWS; `min.insync.replicas`
   y retención), **abortar si `partitions != 1`**, lanzar observador y nodos, barrera
   `READY`, barrera de inicio de clientes, volcar `kafka_log.jsonl` (assign+seek 0),
   invocar `verify_run.py`, escribir `summary.json`/`meta.json`, borrado opcional.
2. `verify_run.py`: V1–V8 (tabla de sección 7) + `expect_violation` para control.
3. `sync_enabled=false`: sin `acquire/release`, resto igual.

Aceptación: `normal` (3 nodos, 5 pedidos) con V1–V7 `PASS`; `control-no-sync` con
`overlaps>0` y marcado como éxito esperado.
MCP: preflight (`cluster_overview`, `describe_topic`, `describe_configs`) y revisión
post-run de grupos; **nunca durante la corrida**.

---

### Fase 5 — Escenarios completos en dev

Entregables: los 5 YAML (`control-no-sync`, `normal`, `high-contention`,
`uneven-load`, `scale-10`) + `aggregate.py` (`aggregate.csv`, `report.md`) +
`extension-semaphore` si hay tiempo.

Tareas:
1. ≥3 repeticiones en `normal` y `high-contention` con `--seed` registrada.
2. `uneven-load` con un cliente `think_time≈[0,0.2]` y más pedidos.
3. `scale-10` (10 nodos; confirmar con el profesor si 10 clientes, 1 por nodo).
4. Agregación: mediana/min/máx de espera, accesos por cliente, mensajes por entrada
   (teórico 2N+2 vs. observado `protocol_published`/`protocol_delivered`).

Aceptación: todos los YAML corren; agregados generados; semillas y versiones en `meta.json`.
MCP: preflight por escenario y comprobación de topics/grupos; no aporta datos.

---

### Fase 6 — Ejecución en AWS

Entregables:
```
tp2/scripts/run_remote_tp2.sh      # calcado de scripts/run_remote_experiment.sh
```

Tareas:
1. Copiar `tp2/` a `monitor`, crear venv con `requirements.txt`, sustituir bootstrap a
   `kafka-1:9092,kafka-2:9092,kafka-3:9092`.
2. `--preflight`: alcanzar brokers, crear/borrar topic de prueba, comprobar `partitions==1`,
   registrar versión de Python y de `confluent-kafka`.
3. ≥3 repeticiones de `normal` y `high-contention`; traer `output/remote-<ts>/`.
4. Vigilar CPU/RAM en Grafana (5–10 nodos + clientes ≈ 1 GB).

**El MCP no se usa contra AWS** (IPs privadas anunciadas). Si se quisiera, habría que
levantar túneles y resolución de los 3 brokers; no aporta frente al preflight remoto.

Aceptación: preflight OK y ≥3 repeticiones en `PASS`.

---

### Fase 7 — Agregación, documentación y entrega

Entregables:
- `tp2/README.md` en **pt-BR**: instalación, ejecución, versiones fijadas, hipótesis
  (sección 3.5), resultados, declaración del broker como secuenciador.
- `tp2/DECISIONES.md` completo.
- Logs seleccionados: un acceso concreto con `queue_head`/`queue_len` que explique
  "por qué entró uno y esperaron los demás".
- `.gitignore` aditivo (`tp2/output/`, `tp2/.venv/`, `*.jsonl` salvo evidencia elegida).
- Dashboard `monitor/grafana/dashboard-tp2.json` (envío/recepción) y runbook de
  Grafana en `tp2/README.md` §8.1 e `tp2/AWS.md` §5.1.
- Checklist de sección 13.

Aceptación: checklist completo; sin credenciales; reproducibilidad (semillas/versiones).

---

## 4. Riesgos y mitigaciones

| Riesgo | Mitigación |
|---|---|
| Python 3.14 local vs. 3.10 en `monitor` | Código compatible 3.10; preflight remoto valida versión; fijar wheel de `confluent-kafka`. |
| Topic auto-creado con 3 particiones | `auto.create.topics.enable=false` en dev + creación explícita + aborto si `partitions != 1`. |
| El MCP contamina el experimento | Regla 1.3: no consumir `r-events-*` durante corridas; MCP fuera del camino medido. |
| `consume_messages` no lee desde offset 0 | Volcado del log con consumer propio (`assign`+`seek(0)`). |
| RF=1 en dev vs. RF=3 en AWS | Parámetros por entorno en YAML; el verificador solo exige `partitions==1`. |
| Clúster AWS inalcanzable desde el host | Todo el smoke real en `monitor`; MCP solo dev. |
| RAM en `monitor` | 5–10 procesos + clientes; medir con Grafana antes de `scale-10`. |
| Nombres de topic acumulados | `r-events-<run_id>` único + borrado opcional al final. |

---

## 5. Orden de trabajo inmediato (siguiente paso)

1. Iniciar/verificar las VMs del TP1 y obtener el **IP público actual** de `monitor`
   (cambia al reiniciar; el privado es estable).
2. Cargar la clave en el agente (`ssh-add`) y confirmar `python3 --version`,
   `getent hosts kafka-1` y alcance a `kafka-1:9092` desde `monitor`.
3. Ejecutar el preflight:
   `tp2/scripts/run_remote_tp2.sh <IP_PUBLICO> all --preflight`.
4. Ejecutar `normal` y `high-contention` (≥3 repeticiones cada uno) y luego el
   resto: `tp2/scripts/run_remote_tp2.sh <IP_PUBLICO> all`.
5. Comparar `output/remote-<ts>/output/agregado/report.md` con los resultados
   locales y registrar versiones/semillas de `meta.json`.
6. Slides 10–15 (fase posterior, fora desta spec).

---

## 6. Estado de avance

| Fase | Estado | Evidencia |
|---|---|---|
| 0 — Reconocimiento | ✅ completada | tabla de la sección 2 |
| 1 — Entorno dev + MCP | ✅ completada | `tp2/output/fase1/smoke_dev.json` (`ok:true`, offsets 0 y 1 en la partición 0) y `tp2/output/fase1/mcp_check.json` (`ok:true`, 9 herramientas) |
| 2 — `QueueState` + pruebas | ✅ completada | `45 passed` (incluye mutex K=1, semáforo K>1 y retry de metadata/configs); simulación de 1000 semillas × 3 configuraciones sin violaciones |
| 3 — Nodo/cliente/observador | ✅ completada | `tp2/output/fase3/demo-20261104-183808-c96b` (normal) y `...-183834-0d81` (control, `overlaps=14`); orden 4.3, digesto igual en 3 nodos |
| 4 — Harness/verificador | ✅ completada | `tp2/output/fase4/normal/normal-20261104-190224-s2026-r1-45a6` (3 nós/5 pedidos, V1–V8 `PASS`) e `tp2/output/fase4/control-no-sync/control-no-sync-20261104-190229-s3101-r1-a4b3` (`overlaps=14`) |
| 5 — Escenarios | ✅ completada | 7 YAML (5 obligatorios + 2 extensión semáforo); **18/18 runs PASS**: `control-no-sync` 3/3, `normal` 3/3, `high-contention` 3/3, `uneven-load` 3/3, `scale-10` 2/2, `extension-semaphore` K=2 2/2, `extension-semaphore-k3` K=3 2/2; `output/agregado/{aggregate.csv,report.md}` |
| 6 — AWS | ✅ completada | Preflight OK; **14/14 runs PASS** (V1–V8) em `tp2/output/remote-20261109-165021/` e revalidado em `tp2/output/remote-20261109-193128/` (RF=3, ISR=2, 3 brokers; brokers `c7i-flex.large` 4 GB + `monitor` `m7i-flex.large` 8 GB, Ubuntu 24.04.5/Python 3.12.3): `control-no-sync` 3/3, `normal` 3/3, `high-contention` 3/3, `uneven-load` 3/3, `scale-10` 2/2 |
| 7 — Entrega | ✅ local (slides pendientes) | `tp2/README.md` pt-BR con hipótesis/resultados y runbook de Grafana (§8.1), `tp2/AWS.md` §5.1, `monitor/grafana/dashboard-tp2.json` (UID `kafka-tp2-envio-recepcion`), `DECISIONES.md` D1–D13, evidencia en `output/agregado/evidence_access.md`; slides son fase posterior |

Artefactos creados/actualizados:

```
tp2/README.md                      tp2/AWS.md
tp2/DECISIONES.md                  tp2/requirements.txt
tp2/pytest.ini                     tp2/docker/docker-compose.dev.yml
tp2/src/common/{messages,clock,jsonl_log,config,net}.py
tp2/src/sync_node/{kafka_sync,node,critical_section,client_server}.py
tp2/src/client/client.py           tp2/src/observer/observer.py
tp2/src/harness/{admin,runtime,verify_run,run_experiment,aggregate}.py
tp2/src/harness/scenarios/*.yaml   (7 escenarios)
tp2/scripts/{demo_local,smoke_dev,check_mcp,select_evidence}.py
tp2/scripts/run_remote_tp2.sh
tp2/tests/unit/*.py                tp2/tests/sim/test_simulation.py
```

Kafka de desarrollo: `tp2-kafka-dev` (apache/kafka:3.7.2) escuchando en
`localhost:9092`, con `num.partitions=3` y auto-creación desactivada.

