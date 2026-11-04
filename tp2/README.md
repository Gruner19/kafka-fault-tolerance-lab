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
vazia), o protocolo com entrega síncrona e uma simulação de 1000 sementes com
5 nós.

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
| `--delete-topic` | apaga o tópico ao final (limpeza) |

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

O modo control é considerado **bem-sucedido quando V1 falha de propósito**
(`overlaps > 0`), provando que a instrumentação enxerga acessos simultâneos.

## 6. Verificação automática (V1–V8)

| ID | Verificação |
|---|---|
| V1 | `max_active ≤ capacity` e `overlaps=0` (no control: `overlaps>0`) |
| V2 | todo pedido tem exatamente um `COMMITTED` no cliente e no nó |
| V3 | nenhum `request_id` tem mais de um `ENTER` |
| V4 | ordem dos `BEGIN` no observador = ordem crescente dos offsets de `ACQUIRE` |
| V5 | todos os nós leram a mesma sequência (SHA-256 dos offsets), com `READY.start_offset=0`, e o despejo do tópico confere |
| V6 | zero `INVARIANT_VIOLATION`; todo `RELEASE` removeu a própria cabeça |
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

Guia detalhado de preparação, IPs, preflight e solução de problemas:
[`AWS.md`](AWS.md).

## 9. Resultados

Ver `output/agregado/report.md` (gerado após os cenários) e
`output/agregado/aggregate.csv`.

## 10. Decisões e limitações

- Decisões detalhadas em [`DECISIONES.md`](DECISIONES.md) (D1–D10).
- O TP2 não implementa falhas, recuperação, replicação de estado nem consenso;
  assume ausência de perdas durante os experimentos, como pede a spec.
- `COMMITTED` significa "terminou o acesso simulado", não persistência.
- O MCP serve apenas para desenvolvimento/verificação de configuração; não
  participa do protocolo nem da evidência.
