#!/usr/bin/env bash
# Executa o TP2 na VM "monitor" da AWS e traz os resultados para o host.
#
# A chave privada NUNCA é copiada para a VM: usa-se o ssh-agent (encaminhado
# com `ssh -A`) exatamente como em `scripts/run_remote_experiment.sh` do TP1.
#
# Uso:
#   ssh-add "$SSH_PRIVATE_KEY"
#   tp2/scripts/run_remote_tp2.sh <IP_PUBLICO_MONITOR> <cenario|all> [--preflight]
#
# Exemplos:
#   tp2/scripts/run_remote_tp2.sh 1.2.3.4 normal
#   tp2/scripts/run_remote_tp2.sh 1.2.3.4 all --preflight
#   TP2_EXTRA_ARGS="--requests 10" tp2/scripts/run_remote_tp2.sh 1.2.3.4 normal
#
# Variáveis de ambiente:
#   SSH_PRIVATE_KEY  chave usada apenas pelo cliente ssh (padrão do TP1)
#   SSH_USER         usuário da VM (padrão: ubuntu)
#   AWS_BOOTSTRAP    brokers privados (padrão: kafka-1:9092,kafka-2:9092,kafka-3:9092)
#   AWS_RF           replication factor do tópico (padrão: 3)
#   AWS_MIN_ISR      min.insync.replicas (padrão: 2)
#   TP2_EXTRA_ARGS   argumentos extras repassados ao harness
#   LOCAL_OUT        diretório local de saída (padrão: tp2/output/remote-<ts>)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TP2_ROOT="$ROOT/tp2"
SSH_KEY="${SSH_PRIVATE_KEY:-/home/gruner-sanchez/Descargas/SSH/kafka-cluster-key.pem}"
SSH_USER="${SSH_USER:-ubuntu}"
AWS_BOOTSTRAP="${AWS_BOOTSTRAP:-kafka-1:9092,kafka-2:9092,kafka-3:9092}"
AWS_RF="${AWS_RF:-3}"
AWS_MIN_ISR="${AWS_MIN_ISR:-2}"
REMOTE_ROOT="${REMOTE_ROOT:-/tmp/sis_distribuidos_tp2}"
LOCAL_OUT="${LOCAL_OUT:-$TP2_ROOT/output/remote-$(date +%Y%m%d-%H%M%S)}"

MONITOR_HOST="${1:-}"
TARGET="${2:-normal}"
MODE="run"
if [[ "${3:-}" == "--preflight" ]]; then
  MODE="preflight"
fi

if [[ -z "$MONITOR_HOST" ]]; then
  echo "Uso: $0 <IP_PUBLICO_MONITOR> <cenario|all> [--preflight]" >&2
  echo "Cenários: control-no-sync normal high-contention uneven-load scale-10" >&2
  exit 2
fi

if [[ "$TARGET" == "all" ]]; then
  SCENARIOS=(control-no-sync normal high-contention uneven-load scale-10)
else
  SCENARIOS=("$TARGET")
fi

for scenario in "${SCENARIOS[@]}"; do
  if [[ ! -f "$TP2_ROOT/src/harness/scenarios/$scenario.yaml" ]]; then
    echo "ERRO: cenário não encontrado: tp2/src/harness/scenarios/$scenario.yaml" >&2
    exit 2
  fi
done

if ! ssh-add -l >/dev/null 2>&1; then
  echo "ERRO: carregue a chave no ssh-agent antes de continuar:" >&2
  echo "  ssh-add \"$SSH_KEY\"" >&2
  echo "A chave privada não é copiada para a VM; o agente é encaminhado com ssh -A." >&2
  exit 1
fi

SSH_OPTS=(-A -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 -i "$SSH_KEY")
REMOTE="$SSH_USER@$MONITOR_HOST"

echo "[1/5] Preparando $REMOTE_ROOT em $MONITOR_HOST ($MODE)"
ssh "${SSH_OPTS[@]}" "$REMOTE" \
  "rm -rf '$REMOTE_ROOT' && mkdir -p '$REMOTE_ROOT'"

echo "[2/5] Copiando tp2/ (sem .venv nem output locais)"
tar -C "$TP2_ROOT" -czf - \
  src scripts requirements.txt pytest.ini \
  | ssh "${SSH_OPTS[@]}" "$REMOTE" "tar -xzf - -C '$REMOTE_ROOT'"

echo "[3/5] Criando venv e instalando versões fixadas"
ssh "${SSH_OPTS[@]}" "$REMOTE" "REMOTE_ROOT='$REMOTE_ROOT' bash -s" <<'REMOTE_SETUP'
set -euo pipefail
cd "$REMOTE_ROOT"
python3 - <<'PY'
import sys
if sys.version_info < (3, 10):
    raise SystemExit(f"Python >= 3.10 é obrigatório; encontrado {sys.version.split()[0]}")
print(f"Python remoto: {sys.version.split()[0]}")
PY
python3 -m venv .venv
.venv/bin/pip install -q --upgrade pip
.venv/bin/pip install -q -r requirements.txt
.venv/bin/python - <<'PY'
import confluent_kafka
print(f"confluent-kafka {confluent_kafka.version()[0]}, librdkafka {confluent_kafka.libversion()[0]}")
PY
REMOTE_SETUP

COMMON_ARGS=(
  --bootstrap "$AWS_BOOTSTRAP"
  --replication-factor "$AWS_RF"
  --min-insync-replicas "$AWS_MIN_ISR"
  --delete-topic
)

if [[ "$MODE" == "preflight" ]]; then
  echo "[4/5] Preflight (brokers + tópico de 1 partição + criação/remoção)"
  for scenario in "${SCENARIOS[@]}"; do
    echo "  - $scenario"
    ssh "${SSH_OPTS[@]}" "$REMOTE" "REMOTE_ROOT='$REMOTE_ROOT' bash -s" -- "$scenario" "${COMMON_ARGS[@]}" <<'REMOTE_PREFLIGHT'
set -euo pipefail
cd "$REMOTE_ROOT"
scenario="$1"
shift
.venv/bin/python src/harness/run_experiment.py \
  --scenario "src/harness/scenarios/$scenario.yaml" \
  --preflight "$@"
REMOTE_PREFLIGHT
  done
else
  echo "[4/5] Executando cenário(s): ${SCENARIOS[*]}"
  EXTRA_ARGS=()
  if [[ -n "${TP2_EXTRA_ARGS:-}" ]]; then
    read -r -a EXTRA_ARGS <<< "$TP2_EXTRA_ARGS"
  fi
  for scenario in "${SCENARIOS[@]}"; do
    echo "  - $scenario"
    ssh "${SSH_OPTS[@]}" "$REMOTE" "REMOTE_ROOT='$REMOTE_ROOT' bash -s" -- \
      "$scenario" "${COMMON_ARGS[@]}" "${EXTRA_ARGS[@]}" <<'REMOTE_RUN'
set -euo pipefail
cd "$REMOTE_ROOT"
scenario="$1"
shift
.venv/bin/python src/harness/run_experiment.py \
  --scenario "src/harness/scenarios/$scenario.yaml" \
  "$@"
REMOTE_RUN
  done

  echo "[5/5] Agregando no monitor"
  ssh "${SSH_OPTS[@]}" "$REMOTE" "REMOTE_ROOT='$REMOTE_ROOT' bash -s" <<'REMOTE_AGG'
set -euo pipefail
cd "$REMOTE_ROOT"
.venv/bin/python src/harness/aggregate.py --root output || true
REMOTE_AGG
fi

mkdir -p "$LOCAL_OUT"
echo "[5/5] Trazendo resultados para $LOCAL_OUT"
ssh "${SSH_OPTS[@]}" "$REMOTE" "tar -C '$REMOTE_ROOT' -czf - output" \
  | tar -C "$LOCAL_OUT" -xzf -

echo "Concluído. Resultados locais: $LOCAL_OUT"
if [[ "$MODE" == "run" ]]; then
  echo "Relatório: $LOCAL_OUT/output/agregado/report.md"
else
  echo "Preflight por cenário em: $LOCAL_OUT/output/<cenário>/preflight.json"
fi
