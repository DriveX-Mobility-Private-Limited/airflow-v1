from airflow.decorators import dag
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from datetime import timedelta
import pendulum


@dag(
    dag_id="airbyte_metax_sales_leads_to_clickhouse_sync",
    schedule=timedelta(hours=1),
    start_date=pendulum.datetime(2026, 2, 1, tz="UTC"),
    catchup=False,
    tags=["airbyte", "metax", "clickhouse", "hourly"],
)

def airbyte_metax_sales_leads_to_clickhouse_sync():

    # 1️⃣ Trigger Airbyte Sync
    sync_data = AirbyteTriggerSyncOperator(
        task_id="trigger_airbyte_sync",
        airbyte_conn_id="airbyte_default",
        connection_id="656cb078-303d-4a02-9302-f881413e48d8",
        asynchronous=True,   # Must be True if using sensor
        timeout=3600,
    )

    # 2️⃣ Wait for Sync Completion
    check_status = AirbyteJobSensor(
        task_id="check_sync_status",
        airbyte_conn_id="airbyte_default",
        airbyte_job_id="{{ ti.xcom_pull(task_ids='trigger_airbyte_sync') }}",
    )

    sync_data >> check_status


airbyte_metax_sales_leads_to_clickhouse_sync()
