output "kafka_private_ips" {
  description = "IPs privadas de los 3 brokers"
  value       = [for ip in aws_instance.kafka[*].private_ip : ip]
}

output "kafka_public_ips" {
  description = "IPs públicas de los 3 brokers"
  value       = [for ip in aws_instance.kafka[*].public_ip : ip]
}

output "monitor_ip" {
  description = "IP privada y pública de la VM de monitoreo"
  value = {
    private = aws_instance.monitor.private_ip
    public  = aws_instance.monitor.public_ip
  }
}

output "cluster_id" {
  description = "ID del clúster KRaft"
  value       = var.cluster_id
}

output "tunnels" {
  description = "Comandos de túnel SSH para acceder a Grafana/Prometheus desde local"
  value = format(
    <<-EOT
    # Grafana (dashboard principal)
    ssh -L 3000:localhost:3000 -N -i %s ubuntu@%s
    # Prometheus (API de métricas)
    ssh -L 9090:localhost:9090 -N -i %s ubuntu@%s
    # kafka_exporter (consumer lag)
    ssh -L 9308:localhost:9308 -N -i %s ubuntu@%s
    EOT,
    var.private_key_file, aws_instance.monitor.public_ip,
    var.private_key_file, aws_instance.monitor.public_ip,
    var.private_key_file, aws_instance.monitor.public_ip
  )
}

output "run_ansible" {
  description = "Cómo configurar el clúster tras el aprovisionamiento"
  value = "cd ansible && ansible-playbook -i inventories/hosts.yml site.yml"
}