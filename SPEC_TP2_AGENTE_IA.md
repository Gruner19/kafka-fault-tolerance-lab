# SPEC TP2 — Implementación de B6 (ordenación de accesos con Kafka) sobre el sistema del TP1

> **Destinatario:** agente de IA que implementa el código. **Autor humano:** Gruner Antonio Sanchez Morales (grupo individual).
> **Curso:** BCC362 Sistemas Distribuídos (UFOP), prof. Joubert Lima. **Seminario:** 09–13/11/2026.
> **Idioma:** esta spec está en español. Todo artefacto entregable al profesor (`tp2/README.md`, comentarios de cara al usuario, mensajes de log legibles, slides) va en **portugués de Brasil**. Identificadores de código y nombres de eventos en inglés/como indica el enunciado.

---

## 0. Cómo usar este documento

1. **Lee primero el repositorio existente (TP1). No asumas nada que esté marcado como "VERIFICAR".** Los puntos marcados así dependen de archivos que esta spec no puede ver.
2. Implementa por fases (sección 12). Al terminar cada fase, ejecuta sus criterios de aceptación antes de seguir.
3. **No modifiques archivos del TP1** salvo cambios aditivos mínimos (p. ej. `.gitignore`). Todo el código nuevo va en `tp2/`.
4. Si una decisión no está aquí, elige la opción más simple que respete las secciones 2 y 3 y regístrala en `tp2/DECISIONES.md`.

### VERIFICAR en el repo existente (antes de escribir código)

| Qué | Dónde mirar | Por qué importa |
|---|---|---|
| Librería cliente Kafka y versión | `scripts/app/requirements.txt`, `order-producer.py` | `kafka-python` **no soporta producer idempotente**; ver sección 5 |
| Versión de Python en `monitor` | SSH a la VM, `python3 --version` | Define sintaxis y dependencias |
| Cómo `kafka-1` resuelve a IP privada desde `monitor` | `/etc/hosts` del monitor, rol Ansible `monitoring`, `scripts/run_remote_experiment.sh` | Los clientes deben correr donde esas IP son alcanzables |
| Cómo `run_remote_experiment.sh` copia scripts, crea venv y trae resultados | `scripts/run_remote_experiment.sh` | TP2 debe reutilizar ese patrón |
| Convenciones de salida (`summary.json`, `raw_series.csv`, `report.html`) | `experiments/experiment_runner.py`, `output/` | Mantener el mismo estilo |
| Security Groups / puertos abiertos | `terraform/main.tf` | Si todo corre en `monitor` no hay que abrir nada nuevo |

---

## 1. Contexto: lo que ya existe (TP1)

- **Infra AWS EC2**, 4 VMs `t3.medium` (2 vCPU, 4 GB), Ubuntu 22.04: `kafka-1/2/3` (broker+controller KRaft, Kafka **3.7.2**) y `monitor` (Prometheus 2.48.0, Grafana, kafka-exporter 1.7.0, JMX exporter 1.0.1).
- Configuración del clúster: `default.replication.factor=3`, `min.insync.replicas=2`, **`num.partitions=3`** (¡trampa! ver 5.1), listeners `PLAINTEXT:9092`, `CONTROLLER:9093`.
- Despliegue: Terraform (opcional) + Ansible (`ansible/site.yml`, roles `broker` y `monitoring`).
- Kafka **anuncia IPs privadas**: los clientes deben correr dentro de la VPC (en `monitor`), nunca desde la máquina local. Acceso a Grafana/Prometheus por túnel SSH (`scripts/tunnels.sh`).
- Ejecución de experimentos: `scripts/run_remote_experiment.sh <IP_MONITOR> <escenario>.yaml` copia scripts al monitor, crea venv temporal, ejecuta, y trae resultados a `output/remote-<timestamp>/`. **La clave privada nunca se copia al monitor.**
- Las IP públicas cambian al reiniciar las VMs; las privadas son estables (`172.31.x.x`). Pasar siempre la IP del monitor como parámetro, no hardcodearla.
- Limitación conocida: `kafka_server_brokertopicmetrics_messagesin_total` **no tiene etiqueta `topic`** → no sirve para aislar tráfico del protocolo.

**Reutilización permitida por el enunciado:** infraestructura del TP1 sí; pero "la instalación no sustituye la demostración de sincronización". El foco del TP2 es el algoritmo, las pruebas de propiedades y los experimentos.

---

## 2. Requisitos del enunciado (obligatorios)

### 2.1 Escenario común "Cluster Sync"
- **Mínimo 5 clientes y 5 procesos del Cluster Sync**, en procesos o contenedores **separados**, comunicándose por red. Prohibido: memoria compartida entre nodos, mutex local como mecanismo de coordinación entre nodos, hilos de un solo proceso simulando nodos.
- **Cada cliente conoce un único proceso** del Cluster Sync (su dirección fija).
- Cada pedido tiene `request_id` único = `(client_id, secuencial)`. El timestamp físico puede ir en el log pero **no sustituye** al `request_id` ni al orden del log.
- Cada cliente hace **entre 10 y 50** pedidos de escritura a `R`.
- El proceso: obtiene autorización → ejecuta la sección crítica simulada = `sleep` aleatorio **0,2–1,0 s** → libera → responde `COMMITTED` al cliente. El cliente espera **1–5 s** aleatorios y hace el siguiente pedido.
- `COMMITTED` significa "terminó el acceso simulado", **no** persistencia ni replicación.
- Sin caídas ni pérdidas de mensajes durante los experimentos. **No** implementar ni evaluar fallos, replicación, replay de recuperación ni consenso.

### 2.2 API obligatoria
Código organizado con operaciones equivalentes a:
```
acquire(R, request_id)
release(R, request_id)
```
**Separadas** de la sección crítica simulada (módulo `critical_section` intercambiable; en el TP3 será reemplazada por acceso al Cluster Store — mantener la interfaz limpia).

### 2.3 Mecanismo B6 con Kafka
- Eventos `ACQUIRE`/`RELEASE` de `R` en **una única partición**.
- **Un consumer group distinto por cada proceso** del Cluster Sync, de modo que todos lean la secuencia completa.
- Cada proceso mantiene una **cola privada** de pedidos pendientes; remueve el pedido en `RELEASE`; **autoriza solo al proceso cuyo pedido es el primero pendiente**.
- Cada mensaje identifica **proceso, recurso y `request_id`**.
- Todos los consumidores deben estar **listos antes de la carga**, partiendo de una **posición conocida**.
- El broker transporta y ordena; **los procesos implementan la regla de acceso**.

### 2.4 Qué NO hacer (violaciones del enunciado o de la propuesta)
- No usar Kafka transactions, compactación u otro mecanismo como "lock" — el lock lo implementa la regla de la cola.
- No usar Redlock / `SET NX` / ZooKeeper / etcd para autorizar.
- No usar más de 1 partición para `R` (el orden de Kafka es solo por partición).
- No hacer que el observador autorice, bloquee ni coordine nada.
- No restar timestamps de hosts distintos para medir esperas.
- No meter credenciales, `.pem`, `terraform.tfstate` ni `terraform.tfvars` en el repo (el enunciado lo prohíbe explícitamente). Revisar `.gitignore`.

---

## 3. Diseño del algoritmo (decidido)

Resumen: **fila replicada sobre log totalmente ordenado**. Clasificación Singhal propuesta: *basado en permiso*, con orden total delegado al log (sin REPLY ni relojes lógicos). El broker actúa como secuenciador (aceptado en B6; declararlo).

### 3.1 Mensajes en Kafka (payload JSON UTF-8)
```json
{"type":"ACQUIRE","resource":"R","process_id":"sync-3","request_id":"c3-0017"}
{"type":"RELEASE","resource":"R","process_id":"sync-3","request_id":"c3-0017"}
```
Publicar siempre con `partition=0` explícita y `key="R"`. Sin campos adicionales necesarios para el protocolo (se puede añadir `sent_mono_ns` solo para debug; el protocolo no debe depender de él).

### 3.2 Estado por proceso
`queue: list[(offset:int, process_id:str, request_id:str)]` + `seen: set[request_id]` + `Condition` (lock local que protege **solo** estas estructuras internas; permitido por el enunciado).

### 3.3 Reglas (pseudocódigo normativo)
```
acquire(R, rid):
    publish ACQUIRE(R, me, rid) a partition 0        # esperar delivery report (acks=all) → obtener offset
    log ACQUIRE
    esperar (Condition) hasta que queue[0] == (_, me, rid)
    return                                            # el llamador loguea ENTER

on_deliver(record):                                   # hilo consumidor, en orden de offset
    ACQUIRE(q, rid):  si rid en seen → log DUPLICATE_IGNORED y descartar
                      sino seen.add(rid); queue.append((offset, q, rid)); notify_all
    RELEASE(q, rid):  si queue vacía o queue[0] != (_, q, rid) → log INVARIANT_VIOLATION (y remover por rid si existe)
                      sino queue.pop(0); notify_all

release(R, rid):
    publish RELEASE(R, me, rid)                       # SOLO después de registrar END (ver 4.3)
    log RELEASE
```
**Propiedades por construcción** (bajo hipótesis de sección 3.5): exclusión mutua (prefijos del mismo log ⇒ misma cola; `RELEASE` solo tras salir), sin deadlock (un recurso, orden total sin ciclos), sin starvation (FIFO por offset; con 1 pedido pendiente por cliente, espera ≤ (pedidos por delante) × duración CS), orden = orden de llegada al log (no orden de relojes lógicos).

### 3.4 Serialización local (hipótesis "orden en el mismo nodo")
Cada proceso recibe pedidos de sus clientes en una **cola local FIFO** atendida por **un único worker**: un pedido a la vez por proceso (`acquire` → CS → `release` → `COMMITTED`). El mutex local nunca sustituye el protocolo.

### 3.5 Hipótesis a declarar en `tp2/README.md` (slide 16 de la clase)
1. Orden por **una única partición** (no por canales FIFO). `--partitions 1` obligatorio.
2. Entrega confiable asumida (`acks=all`, `min.insync.replicas=2`); duplicados de reintento descartados por `request_id`.
3. N conocido y fijo (5; y 10 en el experimento de escala). El protocolo **no cuenta respuestas**; N solo afecta preparación y costo.
4. Sin fallos (si cae el proceso en la cabeza, nadie avanza; sin timeouts/leases; fuera de alcance).
5. Pedidos del mismo nodo serializados localmente antes del protocolo.
6. Todos los consumidores listos y asignados **antes** de la carga; log leído desde offset 0 de un topic **nuevo por ejecución**; `RELEASE` solo tras `END`; retención del topic > duración del experimento.

---

## 4. Arquitectura de componentes

```
 harness (run_experiment.py)  ── crea topic, lanza todo, barrera de inicio, verifica ──┐
   │                                                                                    │
 client-1..N ──TCP/JSONL──► sync-1..N ──produce/consume──► Kafka topic r-events-<run>   │
                              │  (1 partición, RF=3)                                    │
                              └──BEGIN/END (TCP, con ACK)──► observer ─► observer.jsonl ◄┘
```

### 4.1 Protocolo cliente ↔ proceso
TCP con **JSON por línea** (sin dependencias). Cliente→nodo: `{"type":"REQUEST","client_id":"c3","request_id":"c3-0017"}`. Nodo→cliente: `{"type":"COMMITTED","request_id":"c3-0017"}`. Puertos por defecto `7001+i` (nodo i). Un cliente por conexión persistente.

### 4.2 Observador (instrumentación, **no** protocolo)
- Proceso independiente, TCP/JSONL, **fuera de Kafka** (para que la instrumentación no dependa de lo que se prueba).
- Recibe `BEGIN{process_id,request_id}` y `END{...}`; responde `ACK` **tras registrar**.
- Mantiene contador atómico `active`; en `BEGIN`: `active+=1`; si `active>1` → registra `OVERLAP` con los IDs en conflicto; en `END`: `active-=1`. Registra `max_active`, `overlaps` y un número de secuencia monotónico de llegada.
- **No concede permiso, no bloquea un BEGIN ajeno, no es mutex.** Parametrizable `capacity` (1 para `R`; K para la extensión semáforo).

### 4.3 Secuencia de un acceso en el nodo (orden estricto)
```
REQUEST (llega del cliente) → acquire() [ACQUIRE publicado, cola, espera] → ENTER
 → BEGIN al observador (esperar ACK) → sleep U(0.2,1.0) → END al observador (esperar ACK)
 → EXIT → release() [publica RELEASE] → COMMITTED al cliente
```
`END` **debe estar registrado (ACK) antes** de publicar `RELEASE`; así el `BEGIN` del siguiente nunca precede al `END` del anterior en ejecuciones correctas.

### 4.4 Modo de control (`sync_enabled=false`)
El nodo omite `acquire/release` (sin esperar cabeza de cola) pero ejecuta el resto igual (BEGIN/sleep/END). El observador **debe detectar solapamientos** (`overlaps>0`) bajo alta contención. Es el experimento "protección desactivada" exigido.

### 4.5 Harness (`run_experiment.py`)
1. Crear topic nuevo `r-events-<run_id>` (sección 5.1) y **verificar** `partitions==1`.
2. Lanzar observador, luego N nodos; esperar `READY` de **todos** (canal de control TCP, no el log de Kafka) tras recibir su asignación de partición.
3. Lanzar clientes y dispararlos con **señal de inicio conjunta** (barrera) — requisito de "alta contención".
4. Esperar fin; volcar el topic completo a `kafka_log.jsonl` (offset, timestamp, payload) como evidencia.
5. Ejecutar `verify_run.py`; escribir `summary.json`; opcionalmente borrar el topic.

---

## 5. Configuración Kafka

### 5.1 Topic (crear **explícitamente**; nunca confiar en auto-creación)
```bash
/opt/kafka/bin/kafka-topics.sh --create --topic r-events-<run_id> \
  --bootstrap-server kafka-1:9092 --partitions 1 --replication-factor 3 \
  --config min.insync.replicas=2 --config retention.ms=86400000
```
Alternativa: `AdminClient` desde Python. **El default `num.partitions=3` del TP1 rompería el orden** si el topic se auto-crea. Cada nodo, al arrancar, consulta metadata y **aborta** si `partitions != 1`.

### 5.2 Producer
| Parámetro | Valor | Nota |
|---|---|---|
| `acks` | `all` | durabilidad (mismo criterio TP1) |
| `enable.idempotence` | `true` | exige `confluent-kafka`; con `kafka-python` usar `max_in_flight_requests_per_connection=1` + dedupe por `request_id` |
| `linger.ms` | `0` | minimizar latencia |
| `max.in.flight` | ≤5 (idempotente) / 1 (sin idempotencia) | preserva orden en reintentos |
| partición | `0` explícita | |
| espera | delivery report síncrono | para loguear el offset del `ACQUIRE` |

### 5.3 Consumer
| Parámetro | Valor |
|---|---|
| `group.id` | `sync-<i>-<run_id>` (**distinto por proceso**) |
| `enable.auto.commit` | `false` (no se necesita commit) |
| `auto.offset.reset` | `earliest` (topic nuevo ⇒ offset 0 conocido) |
| `fetch.wait.max.ms` / poll timeout | bajo (10–50 ms) para reducir latencia |
| hilos | **un único hilo consumidor** por proceso; lee en orden de offset |
| listo | tras callback de asignación de la partición 0 ⇒ enviar `READY` |

Un solo consumidor por group ⇒ sin rebalances entre pares; aun así esperar asignación antes de la carga.

### 5.4 Bootstrap
`kafka-1:9092,kafka-2:9092,kafka-3:9092` (como en TP1, desde `monitor`). **Versiones**: fijar y registrar todas (`requirements.txt` con `==`, Kafka 3.7.2, Python, SO). El enunciado exige "fijar las versiones utilizadas".

---

## 6. Logging y evidencia

Formato **JSON Lines**, un archivo por actor. Campos mínimos de cada línea:
```json
{"ts_utc":"2026-11-04T18:00:00.123Z","mono_ns":123456789,"actor":"sync-3","event":"ENTER",
 "request_id":"c3-0017","process_id":"sync-3","offset":412,"queue_head":"c3-0017","queue_len":3,"run_id":"..."}
```
- Eventos obligatorios (nombres exactos): `REQUEST, ACQUIRE, ENTER, EXIT, RELEASE, COMMITTED`; más `BEGIN, END` (observador) y de diagnóstico `DUPLICATE_IGNORED, INVARIANT_VIOLATION, READY`.
- Incluir **offset** del log y estado de cola (posición en cola/cabeza) — el enunciado pide mostrar *por qué entró uno y esperaron los demás*.
- `ts_utc` solo informativo. **Las esperas se calculan con `mono_ns` dentro del mismo proceso.**
- Por run, guardar `meta.json`: config completa, **semillas**, versiones, commit git, duración CS, hostnames.
- Semillas: `--seed` base; semilla por actor derivada determinísticamente y registrada.

---

## 7. Verificador automático (`verify_run.py`)

Entradas: `observer.jsonl`, `node-*.jsonl`, `client-*.jsonl`, `kafka_log.jsonl`. Salida: `verification.json` con `PASS/FAIL` por chequeo.

| # | Chequeo | Criterio |
|---|---|---|
| V1 | Exclusión mutua | `observer.max_active ≤ capacity` y `overlaps == 0` |
| V2 | Completitud | todo `request_id` pedido tiene exactamente un `COMMITTED`; totales = suma de pedidos |
| V3 | Sin doble entrada | ningún `request_id` con más de un `ENTER` |
| V4 | Orden | secuencia de `BEGIN` del observador == orden ascendente de offsets de `ACQUIRE` |
| V5 | Consistencia de colas | las secuencias de offsets entregados a los N consumidores son idénticas (hash por nodo) |
| V6 | Invariantes | cero `INVARIANT_VIOLATION`; `RELEASE` siempre removió la cabeza |
| V7 | Topic | `partitions == 1` |
| V8 | Espera acotada | espera máx. observada ≤ cota teórica reportada (informativo) |

**Modo control** (`sync_enabled=false`): el run se considera *exitoso* si V1 **falla** (`overlaps>0`) — demuestra que la instrumentación detecta accesos simultáneos. Marcar `expect_violation: true` en el escenario.
Recordar (slide 8): "N ejecuciones sin colisión no prueban exclusión mutua"; la prueba es el argumento de 3.3, los experimentos son evidencia.

---

## 8. Experimentos (escenarios YAML en `tp2/harness/scenarios/`)

Parámetros comunes: `num_nodes`, `num_clients`, `requests_per_client` (10–50), `cs_duration=[0.2,1.0]`, `think_time=[1,5]`, `seed`, `sync_enabled`, `repetitions`, `capacity`.

| Escenario | Configuración | Qué evaluar |
|---|---|---|
| `control-no-sync` | `sync_enabled=false`, alta contención | el observador detecta `overlaps>0` |
| `normal` | 5 clientes/5 nodos, intervalos del escenario común, **≥3 repeticiones** | sin solapamiento; todos completan |
| `high-contention` | `think_time=[0,0]` (mantener CS 0,2–1 s), arranque conjunto, **≥3 repeticiones** | sin solapamiento; espera mediana |
| `uneven-load` | un cliente con `think_time≈[0,0.2]` y más pedidos; los demás normales | distribución de accesos y esperas; justicia |
| `scale-10` | 10 nodos (por defecto 10 clientes, 1 por nodo — **confirmar con el profesor**) vs. 5 | espera y tráfico vs. N |
| `extension-semaphore` (opcional) | K=2 y K=3 en etapa separada | nunca más de K accesos simultáneos; uso concurrente real |

**Extensión recomendada: semáforo distribuido.** Encaja con el mismo log: un proceso entra si su pedido está entre los **K primeros** de la cola (las posiciones solo avanzan hacia la cabeza, así que un pedido en CS permanece entre los K primeros ⇒ `active ≤ K`). Usar topic/etapa y observador (`capacity=K`) separados. **No sustituye** el mutex sobre `R`, que sigue siendo obligatorio.

---

## 9. Métricas (en `summary.json` y agregadas por escenario)

Por run: pedidos completados, tiempo total, **espera mediana** (`ENTER.mono − REQUEST.mono`, mismo proceso), min/máx, accesos por cliente, configuración, duración CS, semillas.
Agregado (≥3 repeticiones): mediana/min/máx entre runs → `aggregate.csv` + `report.md`.

**Mensajes por entrada** (comparación teórica vs. observada, slide 15):
- Convención: 1 publicación = 1 mensaje; cada entrega a cada consumidor = 1 mensaje (incluye al propio publicador).
- Teórico: **2N + 2** (2 publicaciones + 2N entregas).
- Medir con **contadores en la aplicación**: `protocol_published`, `protocol_delivered` por nodo. Observado = `(Σpublished + Σdelivered) / entradas`.
- Reportar **aparte**: instrumentación (`BEGIN/END`), aplicación (`REQUEST/COMMITTED`), infraestructura Kafka (acks, replicación RF=3).
- No comparar corridas con distinta duración de CS ni carga.

Referencia teórica (N=5/10/25/50): Central 3; Lamport 3(N−1)=12/27/72/147; Ricart–Agrawala 2(N−1)=8/18/48/98; **B6-Kafka 2N+2=12/22/52/102**.

---

## 10. Estructura de archivos a crear

```
tp2/
├── README.md                    # pt-BR: instalación, ejecución, versiones, hipótesis, resultados
├── DECISIONES.md                # decisiones tomadas por el agente
├── requirements.txt             # versiones fijadas (==)
├── src/
│   ├── common/   (messages.py, jsonl_log.py, clock.py, config.py)
│   ├── sync_node/ (node.py, kafka_sync.py, critical_section.py, client_server.py)
│   ├── client/   (client.py)
│   ├── observer/ (observer.py)
│   └── harness/  (run_experiment.py, verify_run.py, aggregate.py, scenarios/*.yaml)
├── tests/                       # ver sección 11
├── docker/      (Dockerfile, docker-compose.dev.yml)   # Kafka KRaft de 1 nodo SOLO para desarrollo
├── scripts/     (run_remote_tp2.sh)                    # calcado de run_remote_experiment.sh
└── output/      (ignorado por git salvo logs seleccionados)
```
Ejecución final: procesos/contenedores en la VM **`monitor`** (5–10 nodos + clientes Python ≈ 1 GB; vigilar CPU/RAM junto a Prometheus/Grafana — Grafana permite observar el consumo). Desarrollo local: `docker-compose.dev.yml` con un Kafka de un solo broker (**RF=1** solo en dev; en AWS RF=3).

---

## 11. Pruebas del propio código

1. **Unitarias** de la máquina de estados de la cola (`on_deliver`, `acquire` condition): duplicados, `RELEASE` fuera de orden, cabeza vacía.
2. **Simulación sin Kafka**: log falso (lista ordenada) + 5 nodos con consumo en ritmos aleatorios; verificar exclusión mutua y orden sobre muchas semillas.
3. **Integración** contra el Kafka de `docker-compose.dev.yml`: run `normal` reducido (3 nodos, 5 pedidos) con verificador en `PASS`; run `control-no-sync` con `overlaps>0`.
4. **Humo en AWS**: `--dry`/preflight (patrón de `experiment_runner.py --preflight`) comprobando alcance de brokers y creación/borrado del topic.

---

## 12. Fases y criterios de aceptación

| Fase | Entregable | Aceptación |
|---|---|---|
| 0 | Reconocimiento del repo, `DECISIONES.md` inicial | Lista "VERIFICAR" resuelta |
| 1 | Entorno dev (docker-compose, venv) | Producir/consumir 1 mensaje en partición 0 |
| 2 | `kafka_sync.py` + pruebas 1 y 2 | Simulación sin violaciones en ≥1000 semillas |
| 3 | Nodo, cliente, observador, logs | Run local 3 nodos: eventos en orden 4.3 |
| 4 | Harness + verificador + modo control | V1–V7 `PASS` en normal; control detecta `overlaps>0` |
| 5 | Escenarios completos en dev | Todos los YAML corren |
| 6 | Ejecución en AWS (`run_remote_tp2.sh`) | ≥3 repeticiones de `normal` y `high-contention` en `PASS` |
| 7 | Agregación, README pt-BR, logs seleccionados | Checklist de entrega (sección 13) |

---

## 13. Checklist de entrega (del enunciado)

- [ ] Código fuente; scripts de carga; verificador de propiedades
- [ ] `README.md` (pt-BR): instalación y ejecución, dependencias **y versiones**, configuración de procesos/contenedores
- [ ] Logs seleccionados (evidencia de un acceso concreto: por qué entró uno y esperaron los demás)
- [ ] Ejecución con sincronización **desactivada** (overlap detectado) y luego activada
- [ ] ≥3 repeticiones de carga normal y alta contención; semillas registradas
- [ ] Comparación costo teórico (2N+2) vs. observado, separando protocolo/instrumentación
- [ ] Declaración explícita de hipótesis y de que el broker actúa como secuenciador
- [ ] **Sin credenciales** en el repositorio
- [ ] Slides 10–15 (cenario, mecanismo, arquitectura, resultados, discusión) — fase posterior, no parte de esta spec

---

## 14. Decisiones abiertas (por defecto asumidas; confirmar con el profesor)

1. Conteo de mensajes: entregas por consumidor (2N+2) **y** solo publicaciones — reportar ambas.
2. "10 procesos": 10 clientes, 1 por nodo.
3. Clasificación Singhal: permiso con orden total delegado al log.
