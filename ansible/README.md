# Configuración (Fase 2): Kafka KRaft + monitoreo

Apply tras `terraform apply`. Configura, formatea e inicia:

- **brokers** (`roles/broker`): `/etc/hosts`, `server-node<N>.properties`
  (replication-factor=3, min.insync.replicas=2), formato KRaft, systemd,
  agente JMX Prometheus.
- **monitor** (`roles/monitoring`): `prometheus.yml` con las IPs reales de los
  brokers, kafka_exporter, datasource de Grafana.

## Cómo ejecutar
```bash
cd ansible
SSH_PRIVATE_KEY=/home/gruner-sanchez/Descargas/SSH/kafka-cluster-key.pem ansible-playbook -i inventories/hosts.yml site.yml
```

## Verificación rápida
```bash
/opt/kafka/bin/kafka-topics.sh --describe \
  --topic orders --bootstrap-server kafka-1:9092
# esperado: cada partición con Replicas: 1,2,3 / Isr: 1,2,3
```