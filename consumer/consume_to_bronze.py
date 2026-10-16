"""Lee el topic ventas_raw y guarda los mensajes TAL CUAL en el bucket bronze (JSON Lines).

Garantía clave: el offset se confirma (commit) SOLO después de escribir en S3.
Si S3 o este proceso se caen, no se pierde nada: al volver, retoma donde quedó.
Costo aceptado: si el proceso muere justo entre escribir y confirmar, ese lote se
escribe dos veces. Silver lo resuelve deduplicando por event_id.

Uso (desde la raíz del proyecto):
    python consumer/consume_to_bronze.py
"""
import argparse
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Redpanda -> bronze (S3)")
    p.add_argument("--topic", default="ventas_raw")
    p.add_argument("--bootstrap", default="localhost:19092")
    p.add_argument("--group", default="bronze-writer", help="consumer group")
    p.add_argument("--bucket", default="bronze")
    p.add_argument("--batch-size", type=int, default=100, help="mensajes por archivo")
    p.add_argument("--interval", type=float, default=10.0, help="segundos máx. antes de escribir lo acumulado")
    return p.parse_args()


def make_s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ["S3_ROOT_USER"],
        aws_secret_access_key=os.environ["S3_ROOT_PASSWORD"],
        region_name="us-east-1",
        # Timeouts cortos y sin reintentos internos: si S3 cae, lo vemos de inmediato
        config=Config(signature_version="s3v4", connect_timeout=5, read_timeout=15, retries={"max_attempts": 1}),
    )


def object_key() -> str:
    ahora = datetime.now(timezone.utc)
    return f"ventas/ingest_date={ahora:%Y-%m-%d}/batch_{ahora:%H%M%S}_{uuid.uuid4().hex[:8]}.jsonl"


def flush(s3, consumer: Consumer, bucket: str, buffer: list[bytes]) -> None:
    """Escribe el lote en S3 y SOLO si funcionó confirma los offsets en Redpanda."""
    key = object_key()
    s3.put_object(Bucket=bucket, Key=key, Body=b"\n".join(buffer) + b"\n")
    consumer.commit(asynchronous=False)
    print(f"{len(buffer)} mensajes -> s3://{bucket}/{key}")


def main() -> None:
    args = parse_args()
    s3 = make_s3()
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest",  # grupo nuevo: empieza desde el primer mensaje
            "enable.auto.commit": False,      # el commit lo hacemos nosotros, después de S3
        }
    )
    consumer.subscribe([args.topic])
    print(f"Escuchando '{args.topic}' como grupo '{args.group}'. Ctrl+C para detener.")

    buffer: list[bytes] = []
    primer_mensaje = 0.0

    try:
        while True:
            vencido = buffer and (
                len(buffer) >= args.batch_size or time.monotonic() - primer_mensaje >= args.interval
            )
            if vencido:
                try:
                    flush(s3, consumer, args.bucket, buffer)
                    buffer.clear()
                except Exception as e:
                    print(f"No se pudo escribir en S3 ({e}). Reintento en 5s; el offset NO se confirmó.")
                    time.sleep(5)
                continue

            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().fatal():
                    raise KafkaException(msg.error())
                print(f"Aviso de Kafka: {msg.error()}")
                continue

            if not buffer:
                primer_mensaje = time.monotonic()
            buffer.append(msg.value())  # bytes tal cual llegaron: bronze no modifica nada

    except KeyboardInterrupt:
        print("Deteniendo...")
    finally:
        if buffer:
            try:
                flush(s3, consumer, args.bucket, buffer)
            except Exception as e:
                print(f"No se pudo guardar el último lote ({e}); sin commit, se reprocesará al volver.")
        consumer.close()


if __name__ == "__main__":
    main()