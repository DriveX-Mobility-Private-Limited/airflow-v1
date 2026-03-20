from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from datetime import timedelta
import pendulum
import logging
import json

log = logging.getLogger(__name__)

AIRBYTE_CONN_ID = "airbyte_default"

CONNECTOR_IDS = [
    ("C1_MetaX",       "656cb078-303d-4a02-9302-f881413e48d8"),
    ("C2_MetaX",       "961aebe2-330c-4df6-82f2-a306d2f90753"),
    ("C_3_MetaX_Raw",  "7e158f4b-da0a-4def-b5b7-9c64ef6e243c"),
    ("C_4_Pricing",    "9c7e1f5f-7dd8-4ac3-94a3-89eb290d7d30"),
    ("C_5_ZOHO",       "2ac8bc07-837b-4bed-a58c-a7bcd735a5c6"),
    ("C_6_ERPX",       "42edc1e2-ecb5-4093-b816-8ce02925cc93"),
    ("C_7_ERPX",       "e828cfc7-b37f-4fd1-9bbd-f13240fc958b"),
    ("C_8_ERPX",       "e6c6b97c-f61f-4030-a99a-0f0a041a29b8"),
]


def check_connector_sync_status(connector_name: str, connection_id: str, **_context) -> dict:
    """
    Checks the latest sync job status for an Airbyte connection via the REST API.
    Does NOT trigger a new sync — read-only status check only.
    """
    hook = HttpHook(http_conn_id=AIRBYTE_CONN_ID, method="POST")

    response = hook.run(
        endpoint="api/v1/jobs/list",
        data=json.dumps({
            "configTypes": ["sync"],
            "configId": connection_id,
            "pagination": {"pageSize": 1, "rowOffset": 0},
        }),
        headers={"Content-Type": "application/json"},
    )

    data = response.json()
    jobs = data.get("jobs", [])

    if not jobs:
        log.warning("[%s] No sync jobs found for connection_id=%s", connector_name, connection_id)
        result = {"connector": connector_name, "connection_id": connection_id, "status": "NO_JOBS"}
        log.info("[%s] Result: %s", connector_name, result)
        return result

    latest_job = jobs[0].get("job", jobs[0])
    status     = latest_job.get("status", "UNKNOWN")
    job_id     = latest_job.get("id", "UNKNOWN")
    created_at = latest_job.get("createdAt", "UNKNOWN")

    result = {
        "connector":     connector_name,
        "connection_id": connection_id,
        "job_id":        job_id,
        "status":        status,
        "created_at":    created_at,
    }

    log.info("[%s] connection_id=%s  job_id=%s  status=%s  created_at=%s",
             connector_name, connection_id, job_id, status, created_at)
    return result


@dag(
    dag_id="airbyte_check_sync_status",
    schedule=timedelta(hours=1),
    start_date=pendulum.datetime(2026, 2, 1, tz="UTC"),
    catchup=False,
    tags=["airbyte", "status-check", "clickhouse", "hourly"],
)
def airbyte_check_sync_status():
    """
    Checks the latest sync status of 8 Airbyte connectors in sequence.
    No syncs are triggered — read-only status check via Airbyte REST API.
    """
    tasks = []
    for connector_name, connection_id in CONNECTOR_IDS:
        task = PythonOperator(
            task_id=f"check_{connector_name}_status",
            python_callable=check_connector_sync_status,
            op_kwargs={
                "connector_name": connector_name,
                "connection_id":  connection_id,
            },
        )
        tasks.append(task)

    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]


airbyte_check_sync_status()
