#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SSH_KEY="${SSH_PRIVATE_KEY:-/home/gruner-sanchez/Descargas/SSH/kafka-cluster-key.pem}"
SSH_USER="${SSH_USER:-ubuntu}"
MONITOR_HOST="${1:?Uso: $0 MONITOR_IP ESCENARIO [SALIDA_LOCAL]}"
SCENARIO="${2:?Uso: $0 MONITOR_IP ESCENARIO [SALIDA_LOCAL]}"
LOCAL_OUT="${3:-$ROOT/output/remote-$(date +%Y%m%d-%H%M%S)}"
REMOTE_ROOT="/tmp/sis_distribuidos_experiment"
REMOTE_SCENARIO="$(basename "$SCENARIO")"

if ! ssh-add -L >/dev/null 2>&1; then
  echo "ERROR: carga la clave en el SSH agent antes de continuar:" >&2
  echo "  ssh-add \"$SSH_KEY\"" >&2
  exit 1
fi

mkdir -p "$LOCAL_OUT"
ssh -A -o StrictHostKeyChecking=accept-new -i "$SSH_KEY" \
  "$SSH_USER@$MONITOR_HOST" \
  "rm -rf '$REMOTE_ROOT' && mkdir -p '$REMOTE_ROOT/experiments/scenarios' '$REMOTE_ROOT/scripts/app' '$REMOTE_ROOT/output'"

scp -q -o StrictHostKeyChecking=accept-new -i "$SSH_KEY" \
  "$ROOT/experiments/experiment_runner.py" \
  "$SSH_USER@$MONITOR_HOST:$REMOTE_ROOT/experiments/"
scp -q -o StrictHostKeyChecking=accept-new -i "$SSH_KEY" \
  "$ROOT/scripts/app/order-producer.py" \
  "$ROOT/scripts/app/order-consumer.py" \
  "$ROOT/scripts/app/requirements.txt" \
  "$SSH_USER@$MONITOR_HOST:$REMOTE_ROOT/scripts/app/"
scp -q -o StrictHostKeyChecking=accept-new -i "$SSH_KEY" \
  "$ROOT/experiments/scenarios/$SCENARIO" \
  "$SSH_USER@$MONITOR_HOST:$REMOTE_ROOT/experiments/scenarios/"

ssh -A -o StrictHostKeyChecking=accept-new -i "$SSH_KEY" \
  "$SSH_USER@$MONITOR_HOST" \
  "python3 -m venv '$REMOTE_ROOT/.venv' && '$REMOTE_ROOT/.venv/bin/pip' install -q -r '$REMOTE_ROOT/scripts/app/requirements.txt' && sed -i -e 's/bootstrap: localhost:9092/bootstrap: kafka-1:9092/' -e 's/18.225.31.122/172.31.37.6/g' -e 's/3.22.234.84/172.31.36.3/g' -e 's/18.222.141.214/172.31.47.50/g' '$REMOTE_ROOT/experiments/scenarios/$REMOTE_SCENARIO' && '$REMOTE_ROOT/.venv/bin/python' '$REMOTE_ROOT/experiments/experiment_runner.py' --scenario '$REMOTE_ROOT/experiments/scenarios/$REMOTE_SCENARIO' --out '$REMOTE_ROOT/output' --ssh-key '' --ssh-user '$SSH_USER'"

scp -q -o StrictHostKeyChecking=accept-new -i "$SSH_KEY" -r \
  "$SSH_USER@$MONITOR_HOST:$REMOTE_ROOT/output/$([[ "$SCENARIO" == *.yaml ]] && basename "$SCENARIO" .yaml)" \
  "$LOCAL_OUT/"

echo "Resultados copiados en: $LOCAL_OUT"