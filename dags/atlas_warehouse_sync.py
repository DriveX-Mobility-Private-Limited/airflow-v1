# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import logging

import pendulum

from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag
from kubernetes import client as k8s_client
from kubernetes.stream import stream

log = logging.getLogger(__name__)

KUBE_CONN_ID = "kubernetes_default"
DBT_POD_NAME = "atlas-prod-5c44c4f5df-2xjbh"
DBT_NAMESPACE = "atlas"
DBT_PROJECT_DIR = "/app"


def run_dbt_in_pod(**_context) -> str:
    """
    Execs `dbt run` inside the existing dbt-pod via the Kubernetes API.
    Streams stdout/stderr to Airflow logs and raises on non-zero exit.
    """
    hook = KubernetesHook(conn_id=KUBE_CONN_ID)
    api_client = hook.get_conn()
    v1 = k8s_client.CoreV1Api(api_client=api_client)

    command = ["dbt", "run", "--project-dir", DBT_PROJECT_DIR]
    log.info("Executing in pod %s/%s: %s", DBT_NAMESPACE, DBT_POD_NAME, " ".join(command))

    ws_client = stream(
        v1.connect_get_namespaced_pod_exec,
        name=DBT_POD_NAME,
        namespace=DBT_NAMESPACE,
        command=command,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        _preload_content=False,
    )

    stdout_buf: list[str] = []
    stderr_buf: list[str] = []

    while ws_client.is_open():
        ws_client.update(timeout=1)
        if ws_client.peek_stdout():
            chunk = ws_client.read_stdout()
            for line in chunk.splitlines():
                log.info("[dbt] %s", line)
            stdout_buf.append(chunk)
        if ws_client.peek_stderr():
            chunk = ws_client.read_stderr()
            for line in chunk.splitlines():
                log.warning("[dbt stderr] %s", line)
            stderr_buf.append(chunk)

    ws_client.close()

    return_code = ws_client.returncode
    log.info("dbt run exit code: %s", return_code)

    if return_code != 0:
        raise RuntimeError(
            f"dbt run failed with exit code {return_code}.\n"
            f"stderr:\n{''.join(stderr_buf)}"
        )

    log.info("dbt run completed successfully.")
    return "".join(stdout_buf)


@dag(
    dag_id="atlas_warehouse_sync",
    schedule=None,  # triggered by airbyte_check_sync_status via TriggerDagRunOperator
    start_date=pendulum.datetime(2026, 2, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "kubernetes", "warehouse", "atlas"],
)
def atlas_warehouse_sync():
    """
    Triggered by `airbyte_check_sync_status` once all Airbyte syncs succeed.
    Execs `dbt run` inside the atlas pod in Kubernetes to transform data in the warehouse.
    """
    PythonOperator(
        task_id="run_dbt_in_pod",
        python_callable=run_dbt_in_pod,
    )


atlas_warehouse_sync()
