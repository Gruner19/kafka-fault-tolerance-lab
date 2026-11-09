# Decisões do TP2

Registro das decisões tomadas pelo agente onde a spec não fixa um valor.
Complementa `SPEC_TP2_AGENTE_IA.md`.

## D1 — Cliente Kafka

`confluent-kafka` (librdkafka), fixado em `requirements.txt`. O TP1 já usava
`confluent-kafka` e o produtor idempotente exige essa biblioteca
(`enable.idempotence=true`); `kafka-python` não suporta idempotência.

## D2 — Kafka de desenvolvimento

`docker/docker-compose.dev.yml` sobe **um** broker KRaft (`apache/kafka:3.7.2`),
com `localhost:9092` anunciado. Para reproduzir a armadilha do TP1:

- `num.partitions=3`
- `auto.create.topics.enable=false`
- `default.replication.factor=1`

RF=1 é **somente** para desenvolvimento. Na AWS, RF=3 e `min.insync.replicas=2`.

## D3 — Tópico por execução

Cada execução cria explicitamente `r-events-<run_id>` com **1 partição**,
`cleanup.policy=delete`, `retention.ms=86400000` e nunca depende de auto-criação.
O harness aborta se `partitions != 1`.

## D4 — MCP de Kafka

O MCP (`kafka-mcp-server`) é usado apenas para **desenvolvimento e verificação de
configuração** contra o Kafka local. Ele não cria tópicos, não devolve offsets e
não expõe `group.id`/`seek`; portanto não participa do protocolo, da medição nem
da geração de evidência.

## D5 — Compatibilidade de Python

Python local: 3.14. Python na `monitor` (Ubuntu 24.04.5 na execução AWS
registrada): 3.12.3; versão mínima suportada: 3.10.
O código deve rodar em **3.10+** (sem `match`, sem PEP 695; usar
`from __future__ import annotations`).

## D6 — Configuração por cenário

Parâmetros de execução vêm de YAML em `tp2/src/harness/scenarios/`, com sementes
registradas em `meta.json`. Nada de valores de tempo fixos no código.

## D7 — Segurança

Nenhuma credencial, chave `.pem`, `terraform.tfstate` ou `terraform.tfvars` no
repositório. As chaves continuam fora da árvore versionada.

## D8 — RELEASE duplicado

A spec (3.3) manda registrar `INVARIANT_VIOLATION` quando um `RELEASE` não
remove a cabeça (K=1) ou não está dentro da janela autorizada (K>1). Para não
gerar falso positivo em reentregas de retry (3.5), um `RELEASE` cujo
`request_id` já foi removido legitimamente é tratado como
`DUPLICATE_IGNORED`. `INVARIANT_VIOLATION` fica reservado a `RELEASE` fora de
ordem de um pedido ainda pendente ou a fila vazia.

## D9 — Sementes

Sementes por ator são derivadas com SHA-256 de `"<seed_base>:<actor>"`, e não
com `hash()` (que é aleatório entre processos). Isso mantém reprodutibilidade
entre execuções e entre máquinas.

## D10 — Sucesso do modo control

No modo control (`sync_enabled=false`) o run é bem-sucedido quando **V1 falha de
propósito** (`overlaps > 0`). As demais verificações continuam valendo: os
clientes precisam completar todos os pedidos (V2), não pode haver `ENTER`
duplicado (V3), o tópico precisa ter 1 partição (V7) e nenhum nó pode registrar
violação de invariante (V6). V4 é pulada porque não existem `ACQUIRE`.

## D11 — Agregação entre runs

`aggregate.py` usa a **mediana das medianas por run** (não junta todas as
amostras, o que daria peso diferente a runs com cargas distintas). O mínimo e o
máximo entre runs vêm dos extremos observados. Mensagens por entrada só são
agregadas em runs com `sync_enabled=true`; no control não há publicações.

## D12 — Execução remota

`scripts/run_remote_tp2.sh` passa bootstrap, RF e ISR como argumentos de CLI do
harness, em vez de reescrever os YAML com `sed` como no TP1. Assim o mesmo
arquivo de cenário serve para dev (RF=1) e AWS (RF=3, ISR=2), e o `meta.json`
registra os valores efetivamente usados.

## D13 — Extensão semáforo (K>1)

O mutex sobre `R` continua sendo `capacity=1` e é o entregável obrigatório. A
extensão usa o mesmo log com `capacity=K` no nó e no observador: um pedido entra
se estiver entre as **K primeiras posições** da fila. Como as posições só
diminuem quando alguém à frente libera, um pedido autorizado permanece dentro da
janela até o seu `RELEASE` (≤ K acessos simultâneos). O verificador:
- V1 exige `max_active ≤ K` e `overlaps=0`;
- V4 é pulada, pois entradas simultâneas não têm ordem determinística de `BEGIN`;
- V6 aceita `RELEASE` com `queue_index < K` e mantém, para K=1, a exigência de
  cabeça exata (`queue_head == request_id`).

Cenários `extension-semaphore` (K=2) e `extension-semaphore-k3` (K=3), ambos em
etapa separada e com 2 repetições locais: `max_active` observado igual a K,
`overlaps=0` e zero violações.
