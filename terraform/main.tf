# ---------------------------------------------------------------------------
# Infraestructura AWS EC2 para el clúster Kafka de tolerancia a fallos.
#
# Fase 1 (este archivo): crea 4 instancias + security groups + cloud-init de
#                        instalación de dependencias.
# Fase 2 (ansible/):     configura Kafka/KRaft, formatea y genera
#                        prometheus.yml con las IPs reales.
# ---------------------------------------------------------------------------

locals {
  ssh_public_key = file(var.ssh_pubkey_path)
}

provider "aws" {
  region = var.region
}

# Buscar la AMI Ubuntu LTS más reciente en la región configurada.
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-${var.ubuntu_codename}-amd64-server-*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

locals {
  ubuntu_ami = data.aws_ami.ubuntu.id
}

# ----------------------------- Security Groups -----------------------------

resource "aws_security_group" "kafka" {
  name_prefix = "${var.name_prefix}-kafka-sg"
  description = "Brokers Kafka (22, 9092/9093, 9100)"
  tags = { Name = "${var.name_prefix}-kafka-sg" }

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 9092
    to_port     = 9093
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # replicación + clientes (TP/entornos controlados)
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# 9100 (JMX) accesible solo desde la VM de monitor.
resource "aws_security_group_rule" "kafka_jmx_from_monitor" {
  security_group_id      = aws_security_group.kafka.id
  type                   = "ingress"
  from_port              = 9100
  to_port                = 9100
  protocol               = "tcp"
  source_security_group_id = aws_security_group.monitor.id
}

resource "aws_security_group" "monitor" {
  name_prefix = "${var.name_prefix}-monitor-sg"
  description = "VM de monitoreo (Prometheus/Grafana)"
  tags = { Name = "${var.name_prefix}-monitor-sg" }

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    from_port   = 9090
    to_port     = 9090
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Prometheus (acceso por túnel SSH)
  }
  ingress {
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Grafana (acceso por túnel SSH)
  }
  ingress {
    from_port   = 9308
    to_port     = 9308
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # kafka_exporter (acceso por túnel SSH)
  }
  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Prometheus consulta brokers directamente
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ------------------------- Instancias de broker Kafka ----------------------

resource "aws_instance" "kafka" {
  count         = 3
  instance_type = "t3.medium" # 2 vCPU / 4 GB
  ami           = local.ubuntu_ami

  vpc_security_group_ids = [aws_security_group.kafka.id]

  tags = { Name = "${var.name_prefix}-kafka-${count.index + 1}" }

  user_data = templatefile(
    "${path.module}/cloud-init/kafka-node.sh",
    {
      node_id    = count.index + 1
      kafka_ver  = var.kafka_version
      kafka_dist = var.kafka_dist
      cluster_id = var.cluster_id
      pubkey     = chomp(local.ssh_public_key)
    }
  )

  lifecycle {
    ignore_changes = [ami]
  }
}

# --------------------------- Instancia de monitoreo -------------------------

resource "aws_instance" "monitor" {
  instance_type = "t3.medium"
  ami           = local.ubuntu_ami

  vpc_security_group_ids = [aws_security_group.monitor.id]

  tags = { Name = "${var.name_prefix}-monitor" }

  user_data = templatefile(
    "${path.module}/cloud-init/monitor-node.sh",
    { pubkey = chomp(local.ssh_public_key) }
  )

  lifecycle {
    ignore_changes = [ami]
  }
}

# ------------------- Generación de inventario para Ansible -----------------

resource "null_resource" "inventory" {
  depends_on = [
    aws_instance.kafka[0], aws_instance.kafka[1], aws_instance.kafka[2],
    aws_instance.monitor,
  ]

  provisioner "local-exec" {
    command = join(" ", [
      "bash ${path.module}/render_inventory.sh",
      join(",", aws_instance.kafka[*].private_ip),
      join(",", aws_instance.kafka[*].public_ip),
      aws_instance.monitor.private_ip,
      aws_instance.monitor.public_ip,
    ])
  }
}