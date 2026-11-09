# Guia de execução na AWS (VM `monitor`)

Este guia cobre o que é necessário para **conectar e preparar** a execução do
TP2 nas VMs do TP1. O cluster Kafka (3 brokers, RF=3, `min.insync.replicas=2`)
e a VM `monitor` já vêm da infraestrutura anterior; o TP2 só precisa de acesso
SSH à `monitor`, Python 3.10+ e rede até os brokers.

## 1. Pré-requisitos

| Item | Como verificar |
|---|---|
| Chave SSH fora do repositório | `ls "$SSH_PRIVATE_KEY"` |
| Chave carregada no ssh-agent | `ssh-add -l` |
| IP público atual da `monitor` | console AWS ou `terraform output` |
| Brokers com RF=3 e ISR=2 | `kafka-topics.sh --describe --bootstrap-server kafka-1:9092` |
| `kafka-1..3` resolvendo na `monitor` | `ssh ubuntu@<IP> getent hosts kafka-1 kafka-2 kafka-3` |
| Python 3.10+ na `monitor` | `ssh ubuntu@<IP> python3 --version` |

O IP público muda quando a VM é parada/iniciada. O IP privado (usado em
`advertised.listeners`) é estável. Se o inventário estiver desatualizado,
regenere-o:

```bash
cd terraform
terraform output            # confira IPs públicos/privados
./render_inventory.sh <priv1,priv2,priv3> <pub1,pub2,pub3> <monitor_priv> <monitor_pub>
```

Também é possível atualizar apenas o `ansible_host` da `monitor` em
`ansible/inventories/hosts.yml`; o script do TP2 recebe o IP público como
argumento e não lê o inventário.

## 2. Preparação da `monitor`

O script `tp2/scripts/run_remote_tp2.sh` faz tudo remotamente:

1. copia `tp2/{src,scripts,requirements.txt,pytest.ini}` para
   `/tmp/sis_distribuidos_tp2` (sem `.venv`, sem `output` locais);
2. valida `python3 >= 3.10`;
3. cria um `.venv` temporário e instala as versões fixadas de
   `requirements.txt` (wheels de `confluent-kafka` já trazem librdkafka);
4. executa o harness com bootstrap privado e RF/ISR de produção;
5. agrega os resultados e traz `output/` de volta.

Nada é instalado no sistema da VM e a chave privada **nunca** é copiada: o
script usa `ssh -A` (agent forwarding).

## 3. Smoke (preflight)

```bash
cd /caminho/do/repositorio
export SSH_PRIVATE_KEY=/caminho/fora/do/repo/kafka-cluster-key.pem
ssh-add "$SSH_PRIVATE_KEY"
tp2/scripts/run_remote_tp2.sh <IP_PUBLICO_MONITOR> all --preflight
```

O preflight verifica, dentro da `monitor`:

- brokers visíveis e metadata do cluster;
- criação e remoção de um tópico de **1 partição** com a RF/ISR pedidas;
- versão de Python, `confluent-kafka` e `librdkafka`.

Resultado local: `tp2/output/remote-<timestamp>/output/<cenário>/preflight.json`.

> Se o preflight falhar por timeout de broker, confirme que a `monitor` alcança
> `kafka-1:9092` (mesma VPC) e que os brokers estão no ar. Do host local isso
> **não** funciona: os brokers anunciam IPs privadas.

## 4. Execução completa

```bash
tp2/scripts/run_remote_tp2.sh <IP_PUBLICO_MONITOR> all
```

Cenários executados: `control-no-sync`, `normal`, `high-contention`,
`uneven-load`, `scale-10`. Para um cenário só:

```bash
tp2/scripts/run_remote_tp2.sh <IP_PUBLICO_MONITOR> normal
```

Extensão semáforo (etapa separada, não incluída em `all`):

```bash
tp2/scripts/run_remote_tp2.sh <IP_PUBLICO_MONITOR> extension-semaphore
tp2/scripts/run_remote_tp2.sh <IP_PUBLICO_MONITOR> extension-semaphore-k3
```

Parâmetros úteis:

```bash
# reduzir a carga durante um teste rápido
TP2_EXTRA_ARGS="--requests 10 --think-min 0.1 --think-max 0.2" \
  tp2/scripts/run_remote_tp2.sh <IP> normal

# mudar o diretório local de saída
LOCAL_OUT=/tmp/tp2-aws tp2/scripts/run_remote_tp2.sh <IP> all
```

Cada run cria `r-events-<run_id>` com 1 partição e apaga o tópico ao final
(`--delete-topic`). Os resultados ficam em:

```
tp2/output/remote-<timestamp>/output/
├── <cenário>/<run_id>/{meta,summary,verification}.json, *.jsonl
└── agregado/{aggregate.csv,report.md}
```

### Resultado da execução registrada

Em 2026-11-09, com os 3 brokers e a `monitor` em Ubuntu 24.04.5:
preflight `ok:true` e **14/14 runs PASS** (V1–V8) nos 5 cenários obrigatórios.
Instâncias efetivas: brokers `c7i-flex.large` (2 vCPU / 4 GB) e `monitor`
`m7i-flex.large` (2 vCPU / 8 GB). Os tópicos ficaram com 1 partição, RF=3,
ISR `[1,2,3]` e `min.insync.replicas=2`. Evidência local:
`tp2/output/remote-20261109-165021/`.

## 5. Monitoração de recursos

Na `monitor`, além dos processos do TP2 (5–10 nós + clientes Python), rodam
Prometheus/Grafana. Para `scale-10`, acompanhe CPU/RAM no Grafana durante a
execução. Se houver pressão de memória, reduza `num_clients` ou a carga via
`TP2_EXTRA_ARGS`, registrando a mudança no relatório.

### 5.1 Evidência de envio e recepção (Grafana)

O `BEGIN`/`END` do protocolo vai por TCP ao observador; só `ACQUIRE`/`RELEASE`
são publicados no tópico `r-events-<run_id>` e aparecem nas métricas do Kafka.

1. Abra o túnel para Grafana e Prometheus:

```bash
./scripts/tunnels.sh <IP_PUBLICO_MONITOR>
# equivalente manual:
ssh -i "$SSH_PRIVATE_KEY" -N \
  -L 3000:localhost:3000 \
  -L 9090:localhost:9090 \
  ubuntu@<IP_PUBLICO_MONITOR>
```

2. Em `http://localhost:3000` (`admin`/`admin`), abra o dashboard
   **Kafka TP2 - envio e recepcao** (UID `kafka-tp2-envio-recepcion`).
   O JSON versionado está em `monitor/grafana/dashboard-tp2.json`; para
   reimportar:

```bash
curl -s -u admin:admin -H 'Content-Type: application/json' \
  -X POST http://localhost:3000/api/dashboards/db \
  -d "{\"dashboard\": $(cat monitor/grafana/dashboard-tp2.json), \"overwrite\": true}"
```

3. Rode o cenário mantendo o tópico para inspeção:

```bash
TP2_KEEP_TOPIC=1 tp2/scripts/run_remote_tp2.sh <IP_PUBLICO_MONITOR> normal
```

Sem `TP2_KEEP_TOPIC=1` o runner passa `--delete-topic` e o tópico some ao final
(útil para não acumular lixo, ruim para olhar o Grafana com calma).

> Validado em 2026-11-09: com `TP2_KEEP_TOPIC=1` e 6 pedidos por cliente, o
> tópico permaneceu no cluster e o `kafka_topic_partition_current_offset`
> mostrou `60` (= 2 × 30 acessos), confirmando a série de envio no Prometheus.

4. No filtro **Topic** escolha `r-events-normal-<run_id>` e ajuste a janela.

| Consulta | Evidência |
|---|---|
| `sum(rate(kafka_server_brokertopicmetrics_messagesin_total[30s]))` | mensagens/s chegando aos brokers (envio) |
| `sum(rate(kafka_server_brokertopicmetrics_bytesin_total[30s]))` | bytes/s de entrada |
| `sum(rate(kafka_server_brokertopicmetrics_bytesout_total[30s]))` | bytes/s de fetch entregues aos consumidores (recepção) |
| `sum(kafka_topic_partition_current_offset{topic=~"$topic"})` | log end offset (registros publicados) |
| `kafka_topic_partition_current_offset{topic=~"$topic"}` | offset da partição 0 do tópico |
| `sum by (consumergroup) (kafka_consumergroup_members{...})` | consumer groups por run |

> **Limitação conhecida.** Com `enable.auto.commit=false`,
> `kafka_consumergroup_lag` e `kafka_consumergroup_current_offset` ficam vazios
> e `kafka_consumergroup_members` pode mostrar `0`. A recepção é comprovada por
> **V5** (sequência idêntica em todos os nós, SHA-256 dos offsets) e pelos
> arquivos `kafka_log.jsonl`/`DELIVERED_DIGEST` do run, não pelo lag.

Limpeza do tópico mantido (rodar em um broker, onde existe `kafka-topics.sh`):

```bash
ssh -A -i "$SSH_PRIVATE_KEY" ubuntu@<IP_PUBLICO_KAFKA_1> \
  '/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 \
   --delete --topic r-events-normal-<run_id>'
```

## 6. Solução de problemas

| Sintoma | Causa provável | Ação |
|---|---|---|
| `Connection timed out` no SSH | IP público mudou ou VM parada | atualizar IP / iniciar VM |
| `ssh-add -l` vazio | chave não carregada | `ssh-add "$SSH_PRIVATE_KEY"` |
| preflight não encontra brokers | DNS/`/etc/hosts` da `monitor` ou SG | `getent hosts kafka-1`; conferir SG 9092/9093 |
| `pip install` falha | sem saída para internet | validar NAT/rotas da VPC; alternativa: wheel local |
| `python3 -m venv` falha | pacote `python3-venv` ausente | `sudo apt-get install -y python3-venv` na `monitor` |
| `Python >= 3.10 é obrigatório` | AMI antiga | atualizar Python na `monitor` ou recriar a VM |
| tópico com 3 partições | auto-criação do broker | o harness cria explicitamente e aborta se `partitions != 1` |
| `UNKNOWN_TOPIC_OR_PART` após criar o tópico | metadata ainda em propagação com RF=3 | já há retry em `describe_topic`/`describe_topic_configs`; se persistir, confira ISR e quórum dos controllers |
| Grafana sem lag/consumer offset | TP2 usa `enable.auto.commit=false` | esperado; comprove recepção por V5 + `kafka_log.jsonl` |
| tópico some logo após o run | runner passa `--delete-topic` | use `TP2_KEEP_TOPIC=1` para manter e inspecionar |

## 7. Segurança

- Não versionar `.pem`, `.tfstate`, `.tfvars` nem credenciais.
- O `run_remote_tp2.sh` não contém IPs fixos; recebe o IP por argumento.
- O MCP de Kafka não é usado na AWS (brokers com IPs privadas); o smoke é o
  preflight remoto.
