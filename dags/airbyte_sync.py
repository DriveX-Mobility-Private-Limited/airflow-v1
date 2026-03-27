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
    ("C_3_MetaX_Raw",  "dda53a71-a946-40e2-833b-610a55d8ad63"),
    ("C_4_Pricing",    "e43db444-a790-4eaf-98d5-dd84fd31a710"),
    ("C_5_ZOHO",       "2ac8bc07-837b-4bed-a58c-a7bcd735a5c6"),
    ("C_6_ERPX",       "42edc1e2-ecb5-4093-b816-8ce02925cc93"),
    ("C_7_ERPX",       "e828cfc7-b37f-4fd1-9bbd-f13240fc958b"),
    ("C_8_ERPX",       "e6c6b97c-f61f-4030-a99a-0f0a041a29b8"),
]

_SEP = "=" * 64


# ── Callbacks ────────────────────────────────────────────────────────────────

def _on_trigger_start(context: dict) -> None:
    ti = context["ti"]
    connector = ti.task_id.replace("trigger_", "")
    log.info(_SEP)
    log.info("  [AIRBYTE] Triggering sync")
    log.info("  Connector : %s", connector)
    log.info("  Task      : %s", ti.task_id)
    log.info("  DAG Run   : %s", ti.run_id)
    log.info(_SEP)


def _on_trigger_success(context: dict) -> None:
    ti = context["ti"]
    connector = ti.task_id.replace("trigger_", "")
    log.info(_SEP)
    log.info("  [AIRBYTE] Sync triggered successfully")
    log.info("  Connector : %s", connector)
    log.info("  Job ID    : %s", ti.xcom_pull(task_ids=ti.task_id))
    log.info(_SEP)


def _on_wait_start(context: dict) -> None:
    ti = context["ti"]
    connector = ti.task_id.replace("wait_", "")
    log.info(_SEP)
    log.info("  [AIRBYTE] Waiting for sync to complete")
    log.info("  Connector : %s", connector)
    log.info("  Polling every 30s ...")
    log.info(_SEP)


def _on_wait_success(context: dict) -> None:
    ti = context["ti"]
    connector = ti.task_id.replace("wait_", "")
    log.info(_SEP)
    log.info("  [AIRBYTE] Sync completed successfully")
    log.info("  Connector : %s", connector)
    log.info(_SEP)


def _on_failure(context: dict) -> None:
    ti = context["ti"]
    log.error(_SEP)
    log.error("  [AIRBYTE] TASK FAILED")
    log.error("  Task      : %s", ti.task_id)
    log.error("  DAG Run   : %s", ti.run_id)
    log.error("  Exception : %s", context.get("exception"))
    log.error(_SEP)


def _on_dbt_trigger_start(_context: dict) -> None:
    log.info(_SEP)
    log.info("  [DBT] All Airbyte connectors synced — triggering warehouse transform")
    log.info("  DAG       : atlas_warehouse_sync")
    log.info(_SEP)


def _on_dbt_trigger_success(_context: dict) -> None:
    log.info(_SEP)
    log.info("  [DBT] atlas_warehouse_sync completed successfully")
    log.info("  Pipeline  : Airbyte -> dbt -> ClickHouse DONE")
    log.info(_SEP)


def _on_dbt_trigger_failure(context: dict) -> None:
    log.error(_SEP)
    log.error("  [DBT] atlas_warehouse_sync FAILED")
    log.error("  Exception : %s", context.get("exception"))
    log.error(_SEP)


# ── DAG ──────────────────────────────────────────────────────────────────────

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
    then triggers atlas_warehouse_sync once all syncs succeed.

    Flow per connector:
        trigger_<name>  >>  wait_<name>
                                 └──(all 8)──► trigger_atlas_warehouse_sync
    """
    wait_tasks = []

    for connector_name, connection_id in CONNECTOR_IDS:
        trigger = AirbyteTriggerSyncOperator(
            task_id=f"trigger_{connector_name}",
            airbyte_conn_id=AIRBYTE_CONN_ID,
            connection_id=connection_id,
            asynchronous=True,
            timeout=3600,
            on_execute_callback=_on_trigger_start,
            on_success_callback=_on_trigger_success,
            on_failure_callback=_on_failure,
        )

        wait = AirbyteJobSensor(
            task_id=f"wait_{connector_name}",
            airbyte_conn_id=AIRBYTE_CONN_ID,
            airbyte_job_id=f"{{{{ ti.xcom_pull(task_ids='trigger_{connector_name}') }}}}",
            timeout=3600,
            poke_interval=30,
            on_execute_callback=_on_wait_start,
            on_success_callback=_on_wait_success,
            on_failure_callback=_on_failure,
        )

        trigger >> wait
        wait_tasks.append(wait)

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_atlas_warehouse_sync",
        trigger_dag_id="atlas_warehouse_sync",
        wait_for_completion=True,
        poke_interval=30,
        on_execute_callback=_on_dbt_trigger_start,
        on_success_callback=_on_dbt_trigger_success,
        on_failure_callback=_on_dbt_trigger_failure,
    )

    for wait in wait_tasks:
        wait >> trigger_dbt


airbyte_check_sync_status()
