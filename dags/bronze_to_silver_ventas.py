"""DAG: bronze -> silver para el dataset de ventas.

Airflow solo ORQUESTA (cuándo corre, en qué orden, qué hacer si falla).
La lógica de limpieza vive en transform/ y ya está probada con pytest.

Flujo:  check_bronze  ->  build_silver  ->  quality_gate
"""
import argparse
import sys
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

# En el contenedor, transform/ y schemas/ están montados aquí (ver docker-compose.yml)
PROJECT = "/opt/airflow/project"
sys.path.insert(0, f"{PROJECT}/transform")

BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
PREFIX = "ventas"
MAX_QUARANTINE_RATIO = 0.10  # si más del 10% de los registros van a cuarentena, la corrida falla


@dag(
    dag_id="bronze_to_silver_ventas",
    description="Limpia bronze y reconstruye silver (Parquet) con reglas en schemas/ventas.yaml",
    schedule="*/15 * * * *",              # cada 15 minutos
    start_date=datetime(2026, 10, 1),
    catchup=False,                         # no ejecutar corridas atrasadas
    max_active_runs=1,                     # nunca dos corridas a la vez
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["silver", "ventas"],
)
def bronze_to_silver_ventas():

    @task
    def check_bronze() -> int:
        """Comprueba que S3 responde y que bronze tiene archivos. Si está vacío, omite la corrida."""
        from run_silver import make_s3

        resp = make_s3().list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=f"{PREFIX}/", MaxKeys=1000)
        files = resp.get("KeyCount", 0)
        if files == 0:
            raise AirflowSkipException("bronze está vacío: nada que procesar")
        return files

    @task
    def build_silver(_files: int) -> dict:
        """Reconstruye silver desde bronze. Devuelve el reporte de calidad."""
        from run_silver import make_s3, run

        args = argparse.Namespace(
            rules=f"{PROJECT}/schemas/ventas.yaml",
            bronze_bucket=BRONZE_BUCKET,
            silver_bucket=SILVER_BUCKET,
            prefix=PREFIX,
        )
        return run(make_s3(), args)

    @task
    def quality_gate(report: dict) -> None:
        """Falla la corrida si la calidad es inaceptable (demasiados registros en cuarentena)."""
        rows_in = report["rows_in"]
        ratio = report["quarantined"] / rows_in if rows_in else 0.0
        print(f"Entrada: {rows_in} | silver: {report['rows_out']} | cuarentena: {report['quarantined']} ({ratio:.1%})")
        if ratio > MAX_QUARANTINE_RATIO:
            raise AirflowFailException(
                f"Cuarentena {ratio:.1%} supera el máximo permitido ({MAX_QUARANTINE_RATIO:.0%})"
            )

    quality_gate(build_silver(check_bronze()))


bronze_to_silver_ventas()
