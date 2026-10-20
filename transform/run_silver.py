"""Bronze -> Silver (como script). Más adelante Airflow solo llamará a esta lógica.

Estrategia "full refresh": cada corrida lee TODO bronze y reconstruye silver desde cero.
Bronze nunca se modifica, así que correrlo dos veces da exactamente el mismo resultado.

Uso (desde la raíz del proyecto):
    python transform/run_silver.py
"""
import argparse
import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
from botocore.config import Config
from dotenv import load_dotenv

from clean import clean_records, load_rules

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bronze -> Silver")
    p.add_argument("--rules", default=str(ROOT / "schemas" / "ventas.yaml"))
    p.add_argument("--bronze-bucket", default="bronze")
    p.add_argument("--silver-bucket", default="silver")
    p.add_argument("--prefix", default="ventas", help="carpeta dentro de cada bucket")
    return p.parse_args()


def make_s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ["S3_ROOT_USER"],
        aws_secret_access_key=os.environ["S3_ROOT_PASSWORD"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", connect_timeout=5, read_timeout=30, retries={"max_attempts": 2}),
    )


def read_bronze(s3, bucket: str, prefix: str):
    """Lee todos los .jsonl de bronze. Devuelve (registros, líneas ilegibles, archivos leídos)."""
    records, bad_lines, files = [], [], 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            files += 1
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode("utf-8", errors="replace")
            for line in body.splitlines():
                if not line.strip():
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    bad_lines.append({"reason": "invalid_json", "record": line, "source": obj["Key"]})
    return records, bad_lines, files


def to_parquet_bytes(rows: list[dict]) -> bytes:
    df = pd.DataFrame(rows)
    df["order_date"] = pd.to_datetime(df["order_date"])
    for col in ("quantity", "unit_price", "total"):
        df[col] = df[col].astype("Int64")  # entero que admite nulos
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def run(s3, args: argparse.Namespace) -> dict:
    rules = load_rules(args.rules)
    records, bad_lines, files = read_bronze(s3, args.bronze_bucket, args.prefix)
    clean, quarantine, report = clean_records(records, rules)
    quarantine = bad_lines + quarantine
    report["quarantined"] = len(quarantine)
    if bad_lines:
        report["quarantine_reasons"]["invalid_json"] = len(bad_lines)
    report["bronze_files_read"] = files
    report["run_at_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

    base = args.prefix
    if clean:
        s3.put_object(Bucket=args.silver_bucket, Key=f"{base}/ventas.parquet", Body=to_parquet_bytes(clean))
    qbody = "\n".join(json.dumps(q, ensure_ascii=False) for q in quarantine)
    s3.put_object(Bucket=args.silver_bucket, Key=f"{base}/_quarantine/quarantine.jsonl", Body=(qbody + "\n").encode("utf-8"))
    s3.put_object(
        Bucket=args.silver_bucket,
        Key=f"{base}/_quality_report.json",
        Body=json.dumps(report, indent=2, ensure_ascii=False).encode("utf-8"),
    )
    return report


def main() -> None:
    args = parse_args()
    report = run(make_s3(), args)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nSilver escrito en s3://{args.silver_bucket}/{args.prefix}/")


if __name__ == "__main__":
    main()
