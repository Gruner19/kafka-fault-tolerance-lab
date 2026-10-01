"""
Consumidor para experimentos de tolerancia a fallos.

Lee del topic `orders` dentro de un consumer group y procesa cada pedido
(simula validación de inventario). Imprime offset, partición y un resumen
final (¿hubo mensajes duplicados? ¿sacó todo?).

Estadísticas clave para el análisis de lag y reasignaciones:
  - mensajes_procesados
  - offsets_por_particion  (para detectar duplicados/reprocesos)
  - lag_maximo_observado   (estimación simple vía comando/API, opcional)
"""
import argparse
import json
import time
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError

STATS = {
    "procesados": 0,
    "duplicados": 0,
    "offsets_vistos": [],          # lista de msg,particion exacta
    "ids": [],
    "particiones_asignadas": set(),
    "errores": [],
}


class Interrupt(Exception):
    pass


def _sigint(sig, frame):
    raise Interrupt()


def process_order(order):
    time.sleep(0.05)  # simular trabajo
    order["status"] = "PROCESSED"
    return order


def build_consumer(bootstrap, group, auto_offset_reset="earliest"):
    conf = {
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
        "session.timeout.ms": 45000,
        "max.poll.interval.ms": 300000,
    }
    return Consumer(conf)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="kafka-1:9092")
    ap.add_argument("--group", default="order-processor-group")
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--duration", type=float, default=0,
                    help="0 = hasta Ctrl+C")
    ap.add_argument("--run-id", help="procesar solo pedidos de esta ejecución")
    ap.add_argument("--ids-file", help="archivo donde guardar IDs procesados")
    args = ap.parse_args()

    consumer = build_consumer(args.bootstrap, args.group)
    consumer.subscribe([args.topic])

    print(f"Consumiendo {args.topic} (group={args.group}) vía {args.bootstrap}")
    start = time.time()

    # detección simple de duplicados (mismo (topic,part,offset) visto antes)
    vistos = set()

    try:
        while True:
            if args.duration and (time.time() - start) >= args.duration:
                break
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                STATS["errores"].append(str(msg.error()))
                print(f"✗ {msg.error()}", flush=True)
                continue

            order = json.loads(msg.value().decode("utf-8"))
            if args.run_id and order.get("run_id") != args.run_id:
                continue
            process_order(order)

            token = (msg.topic(), msg.partition(), msg.offset())
            if token in vistos:
                STATS["duplicados"] += 1
            vistos.add(token)

            STATS["procesados"] += 1
            STATS["ids"].append(order["order_id"])
            STATS["offsets_vistos"].append(token)
            STATS["particiones_asignadas"].add(msg.partition())

            print(
                f"✓ {order['order_id'][:8]} | part {msg.partition()} | "
                f"offset {msg.offset()} | proc {STATS['procesados']}",
                flush=True,
            )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    if args.ids_file:
        with open(args.ids_file, "w", encoding="utf-8") as ids_file:
            ids_file.write("\n".join(STATS["ids"]))
            if STATS["ids"]:
                ids_file.write("\n")

    print(f"\nRESUMEN consumidor: {STATS['procesados']} procesados, "
          f"{STATS['duplicados']} duplicados")
    print(f"Particiones cubiertas: {sorted(STATS['particiones_asignadas'])}")
    if STATS["errores"]:
        print("Errores:", STATS["errores"][-5:])


if __name__ == "__main__":
    main()