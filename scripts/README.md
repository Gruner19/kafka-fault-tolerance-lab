# Scripts de soporte

| Script | Descripción |
|---|---|
| `tunnels.sh` | Abre túneles SSH desde tu máquina a la VM `monitor` en EC2. Expone Grafana `3000`, Prometheus `9090`, kafka_exporter `9308` y broker cliente `9092`. |
| `app/order-producer.py` | Publica pedidos en `orders` con `acks=all`. Imprime offset/partición y resumen. |
| `app/order-consumer.py` | Consume `orders` en un consumer group. Detecta duplicados y devuelve resumen. |

## Uso de túneles
```bash
./scripts/tunnels.sh                      # usa la IP del terraform output / inventario
./scripts/tunnels.sh 54.XXX.XXX.28        # o pasa la IP explícita
```
Después:
- Grafana → http://localhost:3000
- Prometheus → http://localhost:9090
- Métricas lag → http://localhost:9308

## Dependencias de las apps
```bash
pip install -r scripts/app/requirements.txt
```