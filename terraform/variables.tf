variable "region" {
  description = "Región de AWS donde se crean las instancias"
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefijo para nombres de recursos"
  type        = string
  default     = "kafka-tp"
}

variable "ssh_pubkey_path" {
  description = "Ruta a la clave pública SSH que se inyecta en las instancias"
  type        = string
  default     = "/home/gruner-sanchez/Descargas/SSH/kafka-cluster-key.pem.pub"
}

variable "kafka_version" {
  description = "Versión de Apache Kafka (KRaft)"
  type        = string
  default     = "3.7.2"
}

variable "kafka_dist" {
  description = "Sufijo de distribución Java de Kafka"
  type        = string
  default     = "2.13"
}

variable "cluster_id" {
  description = "ID del clúster KRaft (genera uno con kafka-storage.sh random-uuid)"
  type        = string
  default     = "MkU3OEVBNTcwNTJENDM2Qk"
}

variable "private_key_file" {
  description = "Ruta al archivo de clave privada para usar en los túneles SSH"
  type        = string
  default     = "/home/gruner-sanchez/Descargas/SSH/kafka-cluster-key.pem"
}

variable "ubuntu_codename" {
  description = "Código de la distribución Ubuntu LTS"
  type        = string
  default     = "22.04"
}