#!/usr/bin/env bash
# Genera el inventario de Ansible (hosts.yml) a partir de las IPs de las
# instancias creadas por Terraform.
#
# Uso:
#   render_inventory.sh <priv1,priv2,priv3> <pub1,pub2,pub3> <monitor_priv> <monitor_pub>
#
# - ansible_host  → IP con la que Ansible se conecta (pública, desde tu máquina)
# - broker_ip     → IP con la que los brokers se anuncian entre sí (privada,
#                   si están en la misma red/segroid).
set -euo pipefail

KAFKA_PRIV_CSV="${1:?faltan IPs privadas de brokers}"
KAFKA_PUB_CSV="${2:?faltan IPs públicas de brokers}"
MONITOR_PRIV="${3:?falta IP privada del monitor}"
MONITOR_PUB="${4:?falta IP pública del monitor}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INV_DIR="${ROOT}/ansible/inventories"
mkdir -p "${INV_DIR}"

IFS=',' read -r -a KAFKA_PRIV <<< "${KAFKA_PRIV_CSV}"
IFS=',' read -r -a KAFKA_PUB <<< "${KAFKA_PUB_CSV}"

cat > "${INV_DIR}/hosts.yml" <<EOF
# Inventario generado automáticamente por terraform (render_inventory.sh)
all:
  vars:
    ansible_user: ubuntu
    ansible_ssh_private_key_file: "{{ lookup('env','SSH_PRIVATE_KEY') | default('/home/gruner-sanchez/Descargas/SSH/kafka-cluster-key.pem', true) }}"
    ansible_python_interpreter: /usr/bin/python3
    kafka_cluster_id: "MkU3OEVBNTcwNTJENDM2Qk"
    kafka_version: "3.7.2"
    kafka_dist: "2.13"
  children:
    brokers:
      hosts:
        kafka-1:
          ansible_host: "${KAFKA_PUB[0]}"
          broker_ip: "${KAFKA_PRIV[0]}"
          node_id: 1
        kafka-2:
          ansible_host: "${KAFKA_PUB[1]}"
          broker_ip: "${KAFKA_PRIV[1]}"
          node_id: 2
        kafka-3:
          ansible_host: "${KAFKA_PUB[2]}"
          broker_ip: "${KAFKA_PRIV[2]}"
          node_id: 3
    monitor:
      hosts:
        monitor:
          ansible_host: "${MONITOR_PUB}"
          broker_ip: "${MONITOR_PRIV}"
EOF

echo "Inventario escrito en ${INV_DIR}/hosts.yml"