#!/usr/bin/env bash
# Cloud-init para la VM de monitoreo.
# Fase 1 (solo instalación): Prometheus, Grafana y exporters (binarios).
# La configuración (prometheus.yml con IPs reales, datasources, dashboards)
# la hace Ansible en la Fase 2 -> roles/monitoring.

set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

echo ">>> [monitor] Iniciando cloud-init..."
apt-get update -y

# Grafana y utilidades
apt-get install -y grafana wget curl tar jq software-properties-common apache2-utils

# Java (necesario para kafka_exporter no; pero por si acaso)
apt-get install -y openjdk-17-jre

# Prometheus
if [ ! -x /opt/prometheus/prometheus ]; then
  cd /opt
  curl -fsSL \
    https://github.com/prometheus/prometheus/releases/download/v2.48.0/prometheus-2.48.0.linux-amd64.tar.gz \
    -o prometheus.tgz
  tar xzf prometheus.tgz
  mv prometheus-2.48.0.linux-amd64 prometheus
  rm -f prometheus.tgz
  mkdir -p /opt/prometheus/data
fi

# kafka_exporter (consumer lag)
if [ ! -x /usr/local/bin/kafka_exporter ]; then
  cd /tmp
  curl -fsSL \
    https://github.com/danielqsj/kafka_exporter/releases/download/v1.7.0/kafka_exporter-1.7.0.linux-amd64.tar.gz \
    -o kex.tgz
  tar xzf kex.tgz
  install -m 0755 kafka_exporter-1.7.0.linux-amd64/kafka_exporter /usr/local/bin/kafka_exporter
  rm -rf kafka_exporter-1.7.0.linux-amd64 kex.tgz
fi

# Usuario de servicios
id monitor >/dev/null 2>&1 || useradd -r -m -s /bin/bash monitor
mkdir -p /opt/prometheus/data /var/lib/grafana
chown -R monitor:monitor /opt/prometheus /var/lib/grafana

# Inyectar clave pública del analista
mkdir -p /home/ubuntu/.ssh
if ! grep -qF "${pubkey}" /home/ubuntu/.ssh/authorized_keys 2>/dev/null; then
  echo "${pubkey}" >> /home/ubuntu/.ssh/authorized_keys
  chown -R ubuntu:ubuntu /home/ubuntu/.ssh
fi

echo ">>> [monitor] Instalación completa. Esperando Fase 2 (Ansible)."