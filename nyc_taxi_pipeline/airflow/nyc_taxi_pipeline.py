"""NYC Yellow Taxi data pipeline.

Triggered manually (or via API) with optional params:
    {"months": ["2023-01", "2023-02"]}  # list of YYYY-MM strings

Static configuration (PROJECT_ID, BUCKET_NAME, BIGQUERY_DATASET) is read
from environment variables so deployments to different environments do
not require code changes.
"""
from __future__ import annotations

import os
import shutil
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.environ["GCP_PROJECT_ID"]  # fail fast if unset
BUCKET_NAME = os.environ.get("CABSTREAM_BUCKET", f"{PROJECT_ID}_data_lake")
BIGQUERY_DATASET = os.environ.get("CABSTREAM_DATASET", "nyc_taxi_data")
DEFAULT_MONTHS = ["2023-01", "2023-02", "2023-03"]
DOWNLOAD_ROOT = "/tmp/nyc_taxi_data"

DOWNLOAD_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_{year}-{month}.parquet"
)

EXTERNAL_TABLE_SCHEMA = [
    {"name": "VendorID", "type": "INTEGER"},
    {"name": "tpep_pickup_datetime", "type": "TIMESTAMP"},
    {"name": "tpep_dropoff_datetime", "type": "TIMESTAMP"},
    {"name": "passenger_count", "type": "INTEGER"},
    {"name": "trip_distance", "type": "FLOAT"},
    {"name": "RatecodeID", "type": "INTEGER"},
    {"name": "store_and_fwd_flag", "type": "STRING"},
    {"name": "PULocationID", "type": "INTEGER"},
    {"name": "DOLocationID", "type": "INTEGER"},
    {"name": "payment_type", "type": "INTEGER"},
    {"name": "fare_amount", "type": "FLOAT"},
    {"name": "extra", "type": "FLOAT"},
    {"name": "mta_tax", "type": "FLOAT"},
    {"name": "tip_amount", "type": "FLOAT"},
    {"name": "tolls_amount", "type": "FLOAT"},
    {"name": "improvement_surcharge", "type": "FLOAT"},
    {"name": "total_amount", "type": "FLOAT"},
    {"name": "congestion_surcharge", "type": "FLOAT"},
]

CREATE_OPTIMIZED_SQL = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.yellow_tripdata`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.external_yellow_tripdata`
"""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _year_month(month: str) -> tuple[str, str]:
    """Split a 'YYYY-MM' string into ('YYYY', 'MM'); raises on bad input."""
    parts = month.split("-", 1)
    if len(parts) != 2 or len(parts[0]) != 4 or len(parts[1]) != 2:
        raise ValueError(f"Expected YYYY-MM, got {month!r}")
    return parts[0], parts[1]


def _run_dir(run_id: str) -> str:
    """Per-run download directory, isolates concurrent runs / retries."""
    safe = run_id.replace(":", "-").replace("+", "-")
    return os.path.join(DOWNLOAD_ROOT, safe)


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def download_taxi_data(**context) -> str:
    """Stream-download NYC Taxi parquets for each requested month."""
    params = context.get("params") or {}
    months = params.get("months") or DEFAULT_MONTHS
    target_dir = _run_dir(context["run_id"])
    os.makedirs(target_dir, exist_ok=True)

    for month in months:
        year, month_num = _year_month(month)
        url = DOWNLOAD_URL.format(year=year, month=month_num)
        local_path = os.path.join(target_dir, f"yellow_tripdata_{year}-{month_num}.parquet")

        with requests.get(url, stream=True, timeout=(10, 120)) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        size = os.path.getsize(local_path)
        if size < 1_000_000:
            raise RuntimeError(f"Suspiciously small download: {local_path} ({size} bytes)")
        print(f"Downloaded {local_path} ({size:,} bytes)")

    return target_dir


def cleanup_download_dir(**context) -> None:
    """Remove the per-run download dir; runs on all_done so it cleans on failure too."""
    target_dir = _run_dir(context["run_id"])
    shutil.rmtree(target_dir, ignore_errors=True)
    print(f"Cleaned {target_dir}")


def _external_source_uris(months: list[str]) -> list[str]:
    return [
        f"gs://{BUCKET_NAME}/yellow_tripdata/yellow_tripdata_{m}.parquet" for m in months
    ]


EXTERNAL_TABLE_RESOURCE = {
    "tableReference": {
        "projectId": PROJECT_ID,
        "datasetId": BIGQUERY_DATASET,
        "tableId": "external_yellow_tripdata",
    },
    "externalDataConfiguration": {
        "sourceFormat": "PARQUET",
        "sourceUris": _external_source_uris(DEFAULT_MONTHS),
        "schema": {"fields": EXTERNAL_TABLE_SCHEMA},
    },
}


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    description="NYC Yellow Taxi batch ingestion (manual / backfill trigger).",
    schedule_interval=None,  # manual trigger; pass `months` via params
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["nyc", "taxi", "data-pipeline"],
    params={"months": DEFAULT_MONTHS},
) as dag:

    download_task = PythonOperator(
        task_id="download_taxi_data",
        python_callable=download_taxi_data,
    )

    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=(
            f"{DOWNLOAD_ROOT}/"
            "{{ run_id | replace(':','-') | replace('+','-') }}/"
            "yellow_tripdata_*.parquet"
        ),
        dst="yellow_tripdata/",
        bucket=BUCKET_NAME,
    )

    create_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        table_resource=EXTERNAL_TABLE_RESOURCE,
    )

    create_optimized_table_task = BigQueryInsertJobOperator(
        task_id="create_optimized_table",
        configuration={
            "query": {"query": CREATE_OPTIMIZED_SQL, "useLegacySql": False}
        },
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_download_dir",
        python_callable=cleanup_download_dir,
        trigger_rule="all_done",
    )

    (
        download_task
        >> upload_to_gcs_task
        >> create_external_table_task
        >> create_optimized_table_task
        >> cleanup_task
    )
