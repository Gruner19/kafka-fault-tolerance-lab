#!/usr/bin/env bash
# Cloud-init para cada broker Kafka.
# Fase 1 (solo instalación): Java + Kafka descargado (no configurado aún).
# La configuración KRaft + formato la realiza Ansible (Fase 2), que conoce
# las IPs reales de todos los nodos.

set -euo pipefail

NODE_ID="${node_id}"
KAFKA_VERSION="${kafka_ver}"
KAFKA_DIST="${kafka_dist}"
KAFKA_DIR="/opt/kafka"
KAFKA_DATA="/var/lib/kafka/data"

echo ">>> [kafka-node ${NODE_ID}] Iniciando cloud-init..."

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y openjdk-17-jdk openjdk-17-jre wget curl tar jq python3-pip acl

# Usuario de sistema para kafka
id kafka >/dev/null 2>&1 || useradd -r -m -s /bin/bash kafka

# Descargar Kafka (si no está)
if [ ! -f "${KAFKA_DIR}/bin/kafka-server-start.sh" ]; then
  echo ">>> Descargando Kafka ${KAFKA_VERSION}..."
  cd /opt
  curl -fsSL \
    "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${KAFKA_DIST}-${KAFKA_VERSION}.tgz" \
    -o kafka.tgz
  tar xzf kafka.tgz
  ln -sfn "kafka_${KAFKA_DIST}-${KAFKA_VERSION}" kafka
  rm -f kafka.tgz
fi

# Datos + permisos
mkdir -p "${KAFKA_DATA}"
chown -R kafka:kafka /opt/kafka "${KAFKA_DATA}"
chmod -R g+rw /opt/kafka "${KAFKA_DATA}"

# Inyectar clave pública del analista (para túneles SSH y Ansible)
AUTHORIZED="/home/ubuntu/.ssh/authorized_keys"
if ! grep -qF "${pubkey}" "${AUTHORIZED}" 2>/dev/null; then
  mkdir -p /home/ubuntu/.ssh
  echo "${pubkey}" >> "${AUTHORIZED}"
  chown -R ubuntu:ubuntu /home/ubuntu/.ssh
fi

java -version
su -s /bin/bash kafka -c "${KAFKA_DIR}/bin/kafka-storage.sh --version" || true

echo ">>> [kafka-node ${NODE_ID}] Instalación completa. Esperando Fase 2 (Ansible)."