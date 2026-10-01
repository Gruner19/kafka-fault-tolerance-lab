#!/usr/bin/env bash
# Abre túneles SSH desde la máquina local hacia la VM de monitoreo en EC2,
# exponiendo Grafana (3000), Prometheus (9090) y kafka_exporter (9308).
#
# Uso: ./scripts/tunnels.sh [monitor_ip]
#      (si no pasas la IP, lee de terraform output o de ansible inventory)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "${HERE}")"

SSH_KEY="${SSH_PRIVATE_KEY:-/home/gruner-sanchez/Descargas/SSH/kafka-cluster-key.pem}"
SSH_USER="${SSH_USER:-ubuntu}"

MONITOR_IP="${1:-}"

# 1) Obtener la IP del monitor si no se pasó explícitamente.
if [ -z "${MONITOR_IP}" ]; then
  if command -v terraform >/dev/null 2>&1 \
     && [ -d "${ROOT}/terraform/.terraform" ]; then
    MONITOR_IP="$(terraform -chdir="${ROOT}/terraform" output -raw monitor_ip 2>/dev/null || true)"
  fi
fi

if [ -z "${MONITOR_IP}" ]; then
  # intento con ansible inventory
  MONITOR_IP="$(grep -A1 '^ *monitor:' "${ROOT}/ansible/inventories/hosts.yml" 2>/dev/null \
    | grep ansible_host | sed 's/.*: *//')"
fi

if [ -z "${MONITOR_IP}" ]; then
  echo "ERROR: No se pudo determinar la IP del monitor." >&2
  echo "Pasa la IP como argumento o ejecuta terraform apply antes." >&2
  exit 1
fi

echo "> Abriendo túneles hacia ${MONITOR_IP}"
echo "  - Grafana      →  http://localhost:3000"
echo "  - Prometheus   →  http://localhost:9090 (API: api/v1/query)"
echo "  - Kafka Exp    →  http://localhost:9308 (métricas lag)"
echo "  - Broker kafka-1 (cliente) → localhost:9092"
echo "  (Ctrl+C para cerrar)"
echo

exec ssh -i "${SSH_KEY}" -o StrictHostKeyChecking=accept-new \
  -L 3000:localhost:3000 \
  -L 9090:localhost:9090 \
  -L 9308:localhost:9308 \
  -L 9092:${BROKER_1_HOST:-kafka-1}:9092 \
  -N "${SSH_USER}@${MONITOR_IP}"