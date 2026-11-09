# TP2 — Exclusión mutua B6 sobre Kafka

Implementação do **Cluster Sync** do TP2: N processos independentes coordenam o
acesso ao recurso `R` usando **uma única partição Kafka** como log totalmente
ordenado e uma **fila replicada** em cada processo. O broker transporta e ordena;
a regra de permissão é implementada pelos processos.

> Documento em pt-BR (entregável). Identificadores, eventos e nomes de campos
> permanecem em inglês, como no código e na spec.

## 1. Ideia do algoritmo

1. Cada pedido publica `ACQUIRE` em `r-events-<run>` (partição 0).
2. Todos os processos pertencem a **consumer groups distintos** e leem a
   sequência completa desde o início.
3. Cada processo mantém a **mesma fila privada** de pedidos pendentes.
4. Um pedido só entra na seção crítica quando é a **cabeça da fila**.
5. Ao terminar, o processo publica `RELEASE`; todos removem o pedido da cabeça.

A ordem total do log é a única fonte de ordenação: não há relógios lógicos,
mensagens `REPLY`, leases, transações Kafka nem compactação. O broker atua como
**sequenciador** (declarado explicitamente), e a exclusão mútua decorre da regra
da fila: se todos aplicam a mesma sequência de eventos, só a cabeça é autorizada.

A seção crítica é um módulo substituível (`sync_node/critical_section.py`): no
TP2 ela apenas dorme `U(0,2, 1,0) s`; no TP3 será trocada pelo Cluster Store.

### 1.1 Hipóteses declaradas

1. A ordem total vem de **uma única partição** (`--partitions 1`), não de canais
   FIFO externos.
2. A entrega é confiável (`acks=all`, `min.insync.replicas=2` em produção);
   duplicatas de retry são descartadas por `request_id`.
3. `N` é conhecido e fixo (5; e 10 no experimento de escala). O protocolo não
   conta respostas; `N` afeta apenas preparação e custo.
4. Não há falhas durante os experimentos. Se o processo na cabeça cair, ninguém
   avança; timeouts/leases e recuperação estão fora do escopo.
5. Pedidos do mesmo nó são serializados localmente antes do protocolo (um
   worker por processo).
6. Todos os consumidores estão atribuídos **antes** da carga, o log é lido desde
   o offset 0 de um tópico novo por execução, `RELEASE` só ocorre após o ACK do
   `END` e a retenção do tópico (24 h) supera a duração do experimento.

O **broker atua como sequenciador**: ele define a ordem do log, mas não concede
permissão. A exclusão mútua é implementada pela regra da fila em cada processo.

## 2. Estrutura

```
tp2/
├── README.md
├── AWS.md                         # preparação/conexão das VMs
├── DECISIONES.md
├── requirements.txt              # versões fixadas (==)
├── pytest.ini
├── docker/docker-compose.dev.yml # Kafka KRaft de 1 nó, só desenvolvimento
├── src/
│   ├── common/                   # mensagens, relógio, log JSONL, config, TCP
│   ├── sync_node/                # fila replicada, nó, servidor de clientes
│   ├── client/                   # cliente de carga
│   ├── observer/                 # instrumentação (fora do protocolo)
│   └── harness/
│       ├── admin.py              # criação/verificação de tópicos
│       ├── run_experiment.py     # harness completo
│       ├── verify_run.py         # V1–V8
│       ├── aggregate.py          # aggregate.csv + report.md
│       └── scenarios/*.yaml
├── scripts/
│   ├── demo_local.py             # demonstração da fase 3
│   ├── smoke_dev.py
│   ├── check_mcp.py
│   ├── select_evidence.py        # evidência legível de um acesso
│   └── run_remote_tp2.sh         # execução na VM monitor (AWS)
└── output/                       # evidência (ignorada pelo git)
```

## 3. Requisitos e instalação

- Python **3.10+**.
- Docker + Docker Compose para o Kafka de desenvolvimento.
- Versões fixadas em `requirements.txt`:
  - `confluent-kafka==2.15.1`
  - `PyYAML==6.0.3`
  - `pytest==9.2.0`

```bash
cd tp2
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
docker compose -f docker/docker-compose.dev.yml up -d
```

O Kafka de desenvolvimento sobe **um** broker KRaft em `localhost:9092`, com
`num.partitions=3`, `auto.create.topics.enable=false` e RF=1. O harness nunca
depende de auto-criação: ele cria `r-events-<run_id>` com **exatamente 1
partição** e aborta se a metadata divergir.

## 4. Execução

### 4.1 Testes

```bash
cd tp2
.venv/bin/python -m pytest -q
```

Cobrem a máquina de estados da fila (duplicatas, `RELEASE` fora de ordem, fila
vazia, janela K>1), o protocolo com entrega síncrona e uma simulação de 1000
sementes com 5 nós.

### 4.2 Demonstração local (fase 3)

```bash
cd tp2
.venv/bin/python scripts/demo_local.py            # modo sincronizado
.venv/bin/python scripts/demo_local.py --no-sync  # modo control
```

### 4.3 Experimento completo

```bash
cd tp2
.venv/bin/python src/harness/run_experiment.py \
  --scenario src/harness/scenarios/normal.yaml
```

Opções úteis:

| Opção | Efeito |
|---|---|
| `--preflight` | valida brokers, Python e criação/remoção de tópico de 1 partição |
| `--repetitions N` | repete o cenário com sementes `seed+0…seed+N-1` |
| `--requests N` | fixa pedidos por cliente (útil em dev) |
| `--bootstrap` | lista de brokers (dev ou AWS) |
| `--replication-factor` / `--min-insync-replicas` | RF/ISR do tópico (AWS: 3/2) |
| `--capacity K` | sobrescreve K (semáforo; use só para a extensão) |
| `--delete-topic` | apaga o tópico ao final (limpeza) |

Extensão semáforo, em etapa separada:

```bash
.venv/bin/python src/harness/run_experiment.py \
  --scenario src/harness/scenarios/extension-semaphore.yaml      # K=2
.venv/bin/python src/harness/run_experiment.py \
  --scenario src/harness/scenarios/extension-semaphore-k3.yaml   # K=3
```

Cada run grava em `output/<cenário>/<run_id>/`:

```
observer.jsonl        BEGIN/END/OVERLAP/SUMMARY
node-<id>.jsonl       REQUEST/ACQUIRE/ENTER/EXIT/RELEASE/COMMITTED/digest
client-<id>.jsonl     REQUEST/COMMITTED
kafka_log.jsonl       despejo completo da partição 0 (assign + seek 0)
meta.json             configuração, sementes, versões, tópico
verification.json     V1–V8
summary.json          métricas do run
*.out.log / *.err.log saída dos processos
```

### 4.4 Agregação

```bash
.venv/bin/python src/harness/aggregate.py --root output
```

Gera `output/agregado/aggregate.csv` e `output/agregado/report.md` com
mediana/mín/máx entre runs, acessos por cliente e mensagens por entrada.

Para extrair um acesso concreto como evidência legível:

```bash
.venv/bin/python scripts/select_evidence.py \
  output/high-contention/<run_id>
```

O markdown gerado mostra `REQUEST → ACQUIRE → ENTER → BEGIN → EXIT → RELEASE →
COMMITTED`, com `queue_head`, `queue_len`, offset, espera local e duração da
seção crítica.

## 5. Cenários

Arquivos em `src/harness/scenarios/`:

| Cenário | Configuração | O que demonstra |
|---|---|---|
| `control-no-sync` | `sync_enabled=false`, alta contenção | o observador detecta `overlaps > 0` |
| `normal` | 5 clientes/5 nós, `CS=0,2–1,0 s`, `think=1–5 s`, 3 repetições | exclusão mútua e completude |
| `high-contention` | `think=0`, `CS=0,2–1,0 s`, 3 repetições | espera sob fila cheia |
| `uneven-load` | `c1` com 40 pedidos e `think≈0–0,2 s`; demais normais | justiça e distribuição |
| `scale-10` | 10 nós/10 clientes, 2 repetições | espera e tráfego vs. N |
| `extension-semaphore` | K=2, 5 nós/5 clientes, 2 repetições | até 2 acessos simultâneos |
| `extension-semaphore-k3` | K=3, 5 nós/5 clientes, 2 repetições | até 3 acessos simultâneos |

Os dois cenários de extensão são **etapas separadas** do mutex sobre `R`: usam
`capacity=K` no nó e no observador, mas não substituem os cenários obrigatórios
com `capacity=1`.

O modo control é considerado **bem-sucedido quando V1 falha de propósito**
(`overlaps > 0`), provando que a instrumentação enxerga acessos simultâneos.

## 6. Verificação automática (V1–V8)

| ID | Verificação |
|---|---|
| V1 | `max_active ≤ capacity` e `overlaps=0` (no control: `overlaps>0`) |
| V2 | todo pedido tem exatamente um `COMMITTED` no cliente e no nó |
| V3 | nenhum `request_id` tem mais de um `ENTER` |
| V4 | ordem dos `BEGIN` no observador = ordem crescente dos offsets de `ACQUIRE` (pulada com `capacity>1`, pois entradas simultâneas não têm ordem fixa) |
| V5 | todos os nós leram a mesma sequência (SHA-256 dos offsets), com `READY.start_offset=0`, e o despejo do tópico confere |
| V6 | zero `INVARIANT_VIOLATION`; com K=1 todo `RELEASE` removeu a própria cabeça; com K>1 todo `RELEASE` estava dentro das K primeiras posições (`queue_index < K`) |
| V7 | tópico com exatamente 1 partição |
| V8 | espera máxima observada vs. cota teórica (informativa) |

Esperas usam **somente** `ENTER.mono_ns − REQUEST.mono_ns` do mesmo processo.
Nunca se subtraem relógios de hosts diferentes.

## 7. Métricas e mensagens por entrada

- **Espera**: mediana/mín/máx por run e entre runs.
- **Acessos por cliente**: detecta inanição/injustiça.
- **Mensagens por entrada**: `(Σ publicadas + Σ entregues) / pedidos`.
  - Convenção: 1 publicação = 1 mensagem; cada entrega a cada consumidor = 1.
  - Teórico B6: **2N + 2** (2 publicações + 2N entregas).
  - Medido com contadores da aplicação (`protocol_published` e
    `protocol_delivered`), separado da instrumentação `BEGIN/END`.

## 8. Execução na AWS

Pré-requisitos (infraestrutura do TP1):

1. Cluster Kafka com 3 brokers (`kafka-1`, `kafka-2`, `kafka-3`) e RF=3.
2. VM `monitor` com Python 3.10+, acesso aos brokers em `9092` e os nomes
   `kafka-1..3` no `/etc/hosts` (o role de monitoring do Ansible já escreve).
3. Security Group liberando `22` a partir do host e `9092/9093` na VPC.
4. IP **público atual** do `monitor` (muda ao parar/iniciar a VM).

```bash
ssh-add "$SSH_PRIVATE_KEY"
tp2/scripts/run_remote_tp2.sh <IP_PUBLICO_MONITOR> all --preflight
tp2/scripts/run_remote_tp2.sh <IP_PUBLICO_MONITOR> all
```

O script copia `tp2/` para `/tmp/sis_distribuidos_tp2` na `monitor`, cria um
`.venv` temporário, instala as versões fixadas e executa com
`--bootstrap kafka-1:9092,kafka-2:9092,kafka-3:9092 --replication-factor 3
--min-insync-replicas 2`. Os resultados voltam para
`tp2/output/remote-<timestamp>/`. **A chave privada nunca sai do host**: usa-se
`ssh -A` (agent forwarding).

> O MCP de Kafka **não** é usado contra a AWS: os brokers anunciam IPs privadas.
> O smoke real é o `--preflight` executado dentro da `monitor`.

### 8.1 Evidência de envio e recepção no Grafana

O TP2 publica no Kafka apenas `ACQUIRE` e `RELEASE`; os eventos `BEGIN`/`END`
vão por TCP ao observador e **não** passam pelo Kafka, portanto não aparecem no
Grafana. Para evidenciar envio e recepção:

1. Abra os túneles SSH (Grafana em `localhost:3000`, Prometheus em `9090`):

```bash
./scripts/tunnels.sh <IP_PUBLICO_MONITOR>
# equivalente manual:
ssh -i "$SSH_PRIVATE_KEY" -N \
  -L 3000:localhost:3000 \
  -L 9090:localhost:9090 \
  ubuntu@<IP_PUBLICO_MONITOR>
```

2. Acesse `http://localhost:3000` (`admin`/`admin`) e abra o dashboard
   **Kafka TP2 - envio e recepcao** (UID `kafka-tp2-envio-recepcion`,
   arquivo `monitor/grafana/dashboard-tp2.json`).

3. Rode um cenário mantendo o tópico para inspeção posterior:

```bash
TP2_KEEP_TOPIC=1 tp2/scripts/run_remote_tp2.sh <IP_PUBLICO_MONITOR> normal
```

4. No dashboard, selecione o tópico `r-events-normal-<run_id>` no filtro
   **Topic** e ajuste a janela de tempo para o intervalo da corrida.

Painéis e o que cada consulta evidencia:

| Painel / PromQL | O que evidencia |
|---|---|
| `sum(rate(kafka_server_brokertopicmetrics_messagesin_total[30s]))` | taxa de mensagens recebidas pelos brokers (envio) |
| `sum(rate(kafka_server_brokertopicmetrics_bytesin_total[30s]))` | bytes de entrada nos brokers |
| `sum(rate(kafka_server_brokertopicmetrics_bytesout_total[30s]))` | bytes de fetch entregues aos consumidores (recepção) |
| `sum(kafka_topic_partition_current_offset{topic=~"$topic"})` | log end offset = total de registros publicados no tópico |
| `kafka_topic_partition_current_offset{topic=~"$topic"}` | offset por tópico/partição (o TP2 usa 1 partição) |
| `sum by (consumergroup) (kafka_consumergroup_members{...})` | consumer groups conhecidos pelo cluster |

> **Cuidado com o lag.** O TP2 usa `enable.auto.commit=false` e não commita
> offsets: `kafka_consumergroup_lag` e `kafka_consumergroup_current_offset`
> ficam **vazios**, e `kafka_consumergroup_members` pode aparecer como `0`.
> Isso **não** significa ausência de recepção. A prova definitiva de recepção é
> o **V5** (todos os nós leram a mesma sequência, conferida por SHA-256 dos
> offsets) mais `kafka_log.jsonl` e `DELIVERED_DIGEST` no diretório do run.

Consulta manual, sem Grafana:

```bash
ssh -i "$SSH_PRIVATE_KEY" ubuntu@<IP_PUBLICO_MONITOR> \
  "curl -s --get http://localhost:9090/api/v1/query \
   --data-urlencode 'query=kafka_topic_partition_current_offset{topic=\"r-events-normal-<run_id>\"}'"
```

O `value` retornado é o log end offset. No cenário `normal` (5 clientes ×
10 pedidos = 50 acessos), o tópico recebe **100 registros**: um `ACQUIRE` e um
`RELEASE` por acesso.

Ao terminar, apague o tópico mantido (execute em qualquer broker, que tem
`kafka-topics.sh`):

```bash
ssh -A -i "$SSH_PRIVATE_KEY" ubuntu@<IP_PUBLICO_KAFKA_1> \
  '/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 \
   --delete --topic r-events-normal-<run_id>'
```

Guia detalhado de preparação, IPs, preflight e solução de problemas:
[`AWS.md`](AWS.md).

## 9. Resultados

Execução local em 2026-11-04 para os cenários obrigatórios e em 2026-11-09
para a extensão semáforo (Kafka 3.7.2, RF=1, Python 3.14.4,
`confluent-kafka==2.15.1`). **18/18 runs PASS** (V1–V8): 14 runs nos 5 cenários
obrigatórios e 4 runs na extensão semáforo (K=2 e K=3).

| Cenário | Runs | Espera mediana entre runs | Espera máx. | Mensagens/entrada | Teórico |
|---|---:|---:|---:|---:|---:|
| `control-no-sync` | 3/3 | 0,0002 s | 0,0045 s | — | — |
| `normal` (N=5) | 3/3 | 0,138 s | 2,814 s | 12,0 | 12 |
| `high-contention` (N=5) | 3/3 | 2,248 s | 3,528 s | 12,0 | 12 |
| `uneven-load` (N=5) | 3/3 | 0,730 s | 2,791 s | 12,0 | 12 |
| `scale-10` (N=10) | 2/2 | 3,132 s | 5,676 s | 22,0 | 22 |
| `extension-semaphore` (K=2) | 2/2 | 0,889 s | 1,737 s | 12,0 | 12 |
| `extension-semaphore-k3` (K=3) | 2/2 | 0,327 s | 0,852 s | 12,0 | 12 |

- **Control**: `max_active=5` e `overlaps=49…55` por run, como esperado sem
  sincronização. Com sincronização, `max_active=1` e `overlaps=0`.
- **Mensagens por entrada**: o contador da aplicação confirma exatamente
  `2N+2` (12 para N=5, 22 para N=10). Instrumentação (`BEGIN/END`), aplicação
  (`REQUEST/COMMITTED`) e tráfego de infraestrutura são reportados à parte.
- **Carga desigual**: `c1` fez 120 acessos (40 por run) contra 32–37 dos demais
  (10–15 por run); a espera mediana ficou em 0,73 s, sem inanição dos clientes
  menores.
- **Escala**: a espera mediana sobe de 2,25 s (N=5, alta contenção) para 3,13 s
  (N=10), coerente com mais pedidos disputando a mesma seção crítica.
- **Extensão semáforo**: o observador registrou `max_active=2` (K=2) e
  `max_active=3` (K=3), sempre com `overlaps=0` e zero violações. A espera cai
  frente ao mutex sob a mesma carga (`think=0`), como esperado com mais
  paralelismo; os cenários obrigatórios permanecem com `capacity=1`.

Relatório completo: `output/agregado/report.md`; dados de máquina:
`output/agregado/aggregate.csv`.

### 9.1 Evidência de um acesso concreto

Run `high-contention-20261104-185234-s4202-r1-c0fe` (`output/agregado/evidence_access.md`):

| Evento | mono_ns (ms) | offset | queue_head | queue_len |
|---|---:|---:|---|---:|
| REQUEST | 6.545.016,419 | — | — | — |
| ACQUIRE | 6.545.336,795 | 1 | c4-0001 | 4 |
| ENTER | 6.545.337,041 | 1 | c4-0001 | 4 |
| EXIT | 6.545.752,339 | 1 | c4-0001 | 5 |
| RELEASE | 6.545.753,710 | 7 | c4-0001 | 5 |
| COMMITTED | 6.545.753,853 | — | — | — |

No observador: `BEGIN` com `obs_seq=3`/`active=1`, `END` com `obs_seq=4`/`active=0`;
seção crítica de 414,7 ms. Quando `c4-0001` entrou, havia 4 pedidos pendentes e a
cabeça era o próprio `c4-0001`; os demais só entraram depois do `RELEASE`, que
foi publicado **após o ACK do `END`**. O offset do `RELEASE` (7) é maior que o
do `ACQUIRE` (1) porque, durante a seção crítica, outros pedidos (incluindo o
próximo do próprio `c3`) já tinham sido publicados.

### 9.2 Execução real na AWS

Execução em 2026-11-09 na infraestrutura de 4 VMs (3 brokers + `monitor`),
com Kafka 3.7.2 em KRaft, **RF=3**, `min.insync.replicas=2` e tópico de 1
partição. Instâncias usadas: 3 brokers `c7i-flex.large` (2 vCPU / 4 GB) e
`monitor` `m7i-flex.large` (2 vCPU / 8 GB). A `monitor` usou Ubuntu 24.04.5,
Python 3.12.3 e `confluent-kafka==2.15.1`. O preflight remoto passou e os
**14/14 runs dos 5 cenários obrigatórios** passaram em V1–V8.

| Cenário | Runs | Espera mediana entre runs | Espera máx. | Mensagens/entrada | Teórico |
|---|---:|---:|---:|---:|---:|
| `control-no-sync` | 3/3 | 0,0001 s | 0,0010 s | — | — |
| `normal` (N=5) | 3/3 | 0,121 s | 2,806 s | 12,0 | 12 |
| `high-contention` (N=5) | 3/3 | 2,354 s | 3,568 s | 12,0 | 12 |
| `uneven-load` (N=5) | 3/3 | 0,790 s | 2,594 s | 12,0 | 12 |
| `scale-10` (N=10) | 2/2 | 3,113 s | 5,623 s | 22,0 | 22 |

O `meta.json` de cada run registra `partition_detail` com réplicas `[1, 2, 3]`
e ISR `[1, 2, 3]`, além de `min.insync.replicas=2` e `cleanup.policy=delete`.
Evidência: `output/remote-20261109-165021/`.

## 10. Decisões e limitações

- Decisões detalhadas em [`DECISIONES.md`](DECISIONES.md) (D1–D13).
- O TP2 não implementa falhas, recuperação, replicação de estado nem consenso;
  assume ausência de perdas durante os experimentos, como pede a spec.
- `COMMITTED` significa "terminou o acesso simulado", não persistência.
- O MCP serve apenas para desenvolvimento/verificação de configuração; não
  participa do protocolo nem da evidência.
