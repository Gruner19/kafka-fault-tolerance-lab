# Aprovisionamiento AWS EC2

Crea 4 instancias (3 brokers Kafka + 1 monitoreo) con sus security groups y
dispara cloud-init de **instalación** de dependencias.

La **configuración** (KRaft, formato, prometheus.yml con IPs reales) la hace
Ansible en una segunda fase.

## Requisitos previos
- AWS CLI configurado (credenciales).
- Clave pública en `/home/gruner-sanchez/Descargas/SSH/kafka-cluster-key.pem.pub` (variable `ssh_pubkey_path`).
- Terraform ≥ 1.5.

## Uso
```bash
cd terraform
terraform init
terraform apply        # crea las instancias (+ cloud-init)
# Terraform escribe ansible/inventories/hosts.yml automáticamente
terraform output -json # IPs y comandos de túnel
```

Después, configurar el clúster (Fase 2):
```bash
cd ../ansible
SSH_PRIVATE_KEY=/home/gruner-sanchez/Descargas/SSH/kafka-cluster-key.pem ansible-playbook -i inventories/hosts.yml site.yml
```

## Variables útiles (terraform/terraform.tfvars)
```hcl
region      = "us-east-1"
name_prefix = "kafka-tp"
ssh_pubkey_path = "/home/gruner-sanchez/Descargas/SSH/kafka-cluster-key.pem.pub"
cluster_id  = "MkU3OEVBNTcwNTJENDM2Qk"   # genera uno propio
```

## Notas
- Instancias `t3.medium` (2 vCPU / 4 GB). Reduce a `t3.small` si quieres ahorrar.
- **Seguridad:** los puertos 9092/9093 están abiertos para el TP. En producción
  restringe el security group.
- El `cluster_id` debe ser **el mismo** en todas las VMs. Genera uno:

```bash
/opt/kafka/bin/kafka-storage.sh random-uuid
```