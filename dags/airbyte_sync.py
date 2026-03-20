from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from datetime import timedelta
import pendulum
import logging

log = logging.getLogger(__name__)

AIRBYTE_CONN_ID = "airbyte_default"

# 9 Airbyte connector (connection) IDs to check — replace placeholders with real IDs
CONNECTOR_IDS = [
    ("C1_MetaX",            "656cb078-303d-4a02-9302-f881413e48d8"),
    ("C2_MetaX",            "961aebe2-330c-4df6-82f2-a306d2f90753"),
    ("C_3_MetaX_Raw",       "7e158f4b-da0a-4def-b5b7-9c64ef6e243c"),
    ("C_4_Pricing",         "9c7e1f5f-7dd8-4ac3-94a3-89eb290d7d30"),
    ("C_5_ZOHO",            "2ac8bc07-837b-4bed-a58c-a7bcd735a5c6"),
    ("C_6_ERPX",            "42edc1e2-ecb5-4093-b816-8ce02925cc93"),
    ("C_7_ERPX",            "e828cfc7-b37f-4fd1-9bbd-f13240fc958b"),
    ("C_8_ERPX",            "e6c6b97c-f61f-4030-a99a-0f0a041a29b8"),
]


def check_connector_sync_status(connector_name: str, connection_id: str, **context) -> dict:
    """
    Checks the latest sync job status for an Airbyte connection.
    Does NOT trigger a new sync — read-only status check only.
    """
    hook = AirbyteHook(airbyte_conn_id=AIRBYTE_CONN_ID)

    # Fetch the most recent sync job for this connection
    response = hook.get_conn().jobs.list(
        config_types=["sync"],
        config_id=connection_id,
        including_jobs=True,
        pagination={"pageSize": 1},
    )

    jobs = response.jobs if hasattr(response, "jobs") else []

    if not jobs:
        log.warning("[%s] No sync jobs found for connection %s", connector_name, connection_id)
        return {"connector": connector_name, "connection_id": connection_id, "status": "NO_JOBS"}

    latest = jobs[0]
    job = latest.job if hasattr(latest, "job") else latest
    status = job.status if hasattr(job, "status") else str(job)

    log.info("[%s] connection_id=%s  latest sync status=%s", connector_name, connection_id, status)
    return {"connector": connector_name, "connection_id": connection_id, "status": status}


@dag(
    dag_id="airbyte_check_sync_status",
    schedule=timedelta(hours=1),
    start_date=pendulum.datetime(2026, 2, 1, tz="UTC"),
    catchup=False,
    tags=["airbyte", "status-check", "clickhouse", "hourly"],
)
def airbyte_check_sync_status():
    """
    Checks the latest sync status of 9 Airbyte connectors in sequence.
    No syncs are triggered — this is a read-only status check.
    """
    tasks = []
    for connector_name, connection_id in CONNECTOR_IDS:
        task = PythonOperator(
            task_id=f"check_{connector_name}_status",
            python_callable=check_connector_sync_status,
            op_kwargs={
                "connector_name": connector_name,
                "connection_id": connection_id,
            },
        )
        tasks.append(task)

    # Chain all tasks sequentially
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]


airbyte_check_sync_status()
