from __future__ import annotations

import logging
from datetime import timedelta

import pendulum

from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag

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


@dag(
    dag_id="airbyte_check_sync_status",
    schedule=timedelta(hours=1),
    start_date=pendulum.datetime(2026, 2, 1, tz="UTC"),
    catchup=False,
    tags=["airbyte", "sync", "clickhouse", "hourly"],
)
def airbyte_check_sync_status():
    """
    Triggers all 8 Airbyte connectors in parallel, waits for each to complete,
    then triggers the dbt_run_after_airbyte_sync DAG once all syncs succeed.

    Flow per connector:
        trigger_<name>  >>  wait_<name>
                                 └──(all 8)──► trigger_dbt_run
    """
    wait_tasks = []

    for connector_name, connection_id in CONNECTOR_IDS:
        trigger = AirbyteTriggerSyncOperator(
            task_id=f"trigger_{connector_name}",
            airbyte_conn_id=AIRBYTE_CONN_ID,
            connection_id=connection_id,
            asynchronous=True,
            timeout=3600,
        )

        wait = AirbyteJobSensor(
            task_id=f"wait_{connector_name}",
            airbyte_conn_id=AIRBYTE_CONN_ID,
            airbyte_job_id=f"{{{{ ti.xcom_pull(task_ids='trigger_{connector_name}') }}}}",
            timeout=3600,
            poke_interval=30,
        )

        trigger >> wait
        wait_tasks.append(wait)

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_run",
        trigger_dag_id="atlas_warehouse_sync",
        wait_for_completion=False,
    )

    for wait in wait_tasks:
        wait >> trigger_dbt


airbyte_check_sync_status()
