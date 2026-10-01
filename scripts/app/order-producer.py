"""
Productor de pedidos para experimentos de tolerancia a fallos.

Publica mensajes en el topic `orders` con 'acks=all' (máxima durabilidad)
y una clave determinística (order_id) para particionamiento estable.

Uso:
    python3 order-producer.py \
        [--bootstrap kafka-1:9092] [--rate 2] [--duration 300] [--topic orders]

Salida compatible con análisis: imprime pedido_id, partición y offset en cada
entregado, y un resumen final con contador de pedidos enviados/quitados.
Exit code 0 si no hubo errores de entrega, 1 si hubo algunos.
"""
import argparse
import json
import random
import signal
import time
import uuid

from confluent_kafka import Producer
from faker import Faker

fake = Faker("es_ES")

STATS = {"enviados": 0, "fallidos": 0, "offsets": [], "errores": [], "ids": []}


class Interrupt(Exception):
    pass


def _sigint(sig, frame):
    raise Interrupt()


def build_producer(bootstrap, client_id):
    conf = {
        "bootstrap.servers": bootstrap,
        "client.id": client_id,
        "acks": "all",                      # confirmación de todas las réplicas ISR
        "enable.idempotence": True,
        "retries": 10,
        "retry.backoff.ms": 1000,
        "delivery.timeout.ms": 30000,
        "message.timeout.ms": 30000,
    }
    return Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        STATS["fallidos"] += 1
        STATS["errores"].append(str(err))
        print(f"✗ ERROR entrega: {err}", flush=True)
    else:
        STATS["enviados"] += 1
        STATS["offsets"].append(msg.offset())
        if msg.key() is not None:
            STATS["ids"].append(msg.key().decode("utf-8"))
        print(
            f"✓ {msg.topic()}[{msg.partition()}] offset {msg.offset()}",
            flush=True,
        )


def generate_order(run_id):
    return {
        "order_id": fake.uuid4(),
        "run_id": run_id,
        "customer": fake.name(),
        "product": fake.random_element(
            elements=("Laptop", "Teléfono", "Tablet", "Monitor", "Teclado")
        ),
        "quantity": random.randint(1, 10),
        "price": round(random.uniform(100, 2000), 2),
        "timestamp": time.time(),
        "status": "CREATED",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="kafka-1:9092")
    ap.add_argument("--rate", type=float, default=2.0,
                    help="mensajes por segundo")
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--duration", type=float, default=0,
                    help="0 = hasta Ctrl+C")
    ap.add_argument("--run-id", default=uuid.uuid4().hex,
                    help="identificador de la ejecución experimental")
    ap.add_argument("--ids-file", help="archivo donde guardar IDs confirmados")
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _sigint)
    producer = build_producer(args.bootstrap, f"order-producer-{args.run_id}")

    print(f"Produciendo en {args.topic} @ {args.rate}/s vía {args.bootstrap} "
          f"(run_id={args.run_id})")
    start = time.time()
    try:
        while True:
            order = generate_order(args.run_id)
            key = order["order_id"]
            producer.produce(
                args.topic,
                key=key,
                value=json.dumps(order),
                callback=delivery_report,
            )
            producer.poll(0)
            if args.duration and (time.time() - start) >= args.duration:
                break
            time.sleep(1.0 / args.rate)
    except Interrupt:
        pass
    finally:
        producer.flush()

    print(f"\nRESUMEN productor: {STATS['enviados']} enviados, "
          f"{STATS['fallidos']} fallidos, {len(STATS['errores'])} errores")
    if args.ids_file:
        with open(args.ids_file, "w", encoding="utf-8") as ids_file:
            ids_file.write("\n".join(STATS["ids"]))
            if STATS["ids"]:
                ids_file.write("\n")
    if STATS["errores"]:
        print("Últimos errores:", STATS["errores"][-5:])
    return 0 if not STATS["fallidos"] else 1


if __name__ == "__main__":
    raise SystemExit(main())