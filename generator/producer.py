"""Envía ventas corrompidas al topic de Redpanda y guarda la versión limpia como verdad.

Uso (desde la raíz del proyecto):
    python generator/producer.py --n 100 --batches 3 --interval 2
"""
import argparse
import json
import random
import time
from pathlib import Path

from confluent_kafka import Producer

from corrupt import corrupt
from generate import generate_sales

TRUTH_DIR = Path(__file__).resolve().parent.parent / "data" / "truth"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generador de ventas con errores para Redpanda")
    p.add_argument("--n", type=int, default=100, help="ventas limpias por lote")
    p.add_argument("--batches", type=int, default=1, help="cuántos lotes enviar")
    p.add_argument("--interval", type=float, default=1.0, help="segundos entre lotes")
    p.add_argument("--seed", type=int, default=None, help="semilla (si no, se elige una al azar)")
    p.add_argument("--topic", default="ventas_raw")
    p.add_argument("--bootstrap", default="localhost:19092")
    return p.parse_args()


def send(producer: Producer, topic: str, record: dict, on_delivery) -> None:
    """Encola un mensaje; si la cola local está llena, espera y reintenta."""
    payload = json.dumps(record, ensure_ascii=False).encode("utf-8")
    while True:
        try:
            # La clave es event_id: los duplicados caen en la misma partición
            producer.produce(topic, key=record["event_id"], value=payload, callback=on_delivery)
            return
        except BufferError:
            producer.poll(0.5)


def main() -> None:
    args = parse_args()
    seed = args.seed if args.seed is not None else random.randrange(1_000_000)
    print(f"Semilla: {seed}  (úsala con --seed para repetir exactamente estos datos)")

    TRUTH_DIR.mkdir(parents=True, exist_ok=True)
    producer = Producer({"bootstrap.servers": args.bootstrap, "client.id": "chaos-generator"})

    errores: list[str] = []

    def on_delivery(err, msg) -> None:
        if err is not None:
            errores.append(str(err))

    for lote in range(args.batches):
        lote_seed = seed + lote
        limpios = generate_sales(args.n, lote_seed)
        sucios, reporte = corrupt(limpios, seed=lote_seed)

        verdad = TRUTH_DIR / f"clean_{lote_seed}.jsonl"
        verdad.write_text(
            "\n".join(json.dumps(r, ensure_ascii=False) for r in limpios) + "\n", encoding="utf-8"
        )

        for registro in sucios:
            send(producer, args.topic, registro, on_delivery)
            producer.poll(0)
        producer.flush()

        print(f"Lote {lote + 1}/{args.batches}: {reporte}")
        if lote < args.batches - 1:
            time.sleep(args.interval)

    if errores:
        print(f"ATENCIÓN: {len(errores)} mensajes no se entregaron. Primer error: {errores[0]}")
    else:
        print("Todos los mensajes fueron entregados.")


if __name__ == "__main__":
    main()