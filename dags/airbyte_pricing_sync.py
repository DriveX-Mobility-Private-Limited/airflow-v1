from airflow.sdk import dag
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from datetime import timedelta
import pendulum
import logging

log = logging.getLogger(__name__)

AIRBYTE_CONN_ID = "airbyte_default"
C4_PRICING_CONNECTION_ID = "9c7e1f5f-7dd8-4ac3-94a3-89eb290d7d30"


def print_sync_result(**context):
    ti = context["ti"]
    job_id = ti.xcom_pull(task_ids="trigger_c4_pricing_sync")

    log.info("=== C_4_Pricing Sync Result ===")
    log.info("Job ID: %s", job_id)

    # Use AirbyteHook (carries connection auth) — endpoint is relative to the
    # base URL already stored in the Airflow connection (e.g. https://host/api/public/v1)
    hook = AirbyteHook(airbyte_conn_id=AIRBYTE_CONN_ID)
    response = hook.run(endpoint=f"jobs/{job_id}")
    job_details = response.json()

    status     = job_details.get("job", {}).get("status", "UNKNOWN")
    created_at = job_details.get("job", {}).get("createdAt", "UNKNOWN")
    updated_at = job_details.get("job", {}).get("updatedAt", "UNKNOWN")
    duration   = job_details.get("job", {}).get("duration", "UNKNOWN")

    log.info("Status     : %s", status)
    log.info("Created at : %s", created_at)
    log.info("Updated at : %s", updated_at)
    log.info("Duration   : %s", duration)
    log.info("Full response: %s", job_details)

    print(f"Job ID      : {job_id}")
    print(f"Status      : {status}")
    print(f"Created at  : {created_at}")
    print(f"Updated at  : {updated_at}")
    print(f"Duration    : {duration}")
    print(f"Full resp   : {job_details}")

    return {"job_id": job_id, "status": status, "details": job_details}


@dag(
    dag_id="airbyte_c4_pricing_trigger_sync",
    schedule=timedelta(hours=1),
    start_date=pendulum.datetime(2026, 2, 1, tz="UTC"),
    catchup=False,
    tags=["airbyte", "pricing", "clickhouse", "hourly"],
)
def airbyte_c4_pricing_trigger_sync():
    """
    Triggers a sync for the C_4_Pricing Airbyte connector,
    waits for completion, then fetches and prints full job details.
    """

    trigger = AirbyteTriggerSyncOperator(
        task_id="trigger_c4_pricing_sync",
        airbyte_conn_id=AIRBYTE_CONN_ID,
        connection_id=C4_PRICING_CONNECTION_ID,
        asynchronous=True,
        timeout=3600,
    )

    wait = AirbyteJobSensor(
        task_id="wait_for_c4_pricing_sync",
        airbyte_conn_id=AIRBYTE_CONN_ID,
        airbyte_job_id="{{ ti.xcom_pull(task_ids='trigger_c4_pricing_sync') }}",
        timeout=3600,
        poke_interval=30,
    )

    print_result = PythonOperator(
        task_id="print_sync_result",
        python_callable=print_sync_result,
    )

    trigger >> wait >> print_result


airbyte_c4_pricing_trigger_sync()
