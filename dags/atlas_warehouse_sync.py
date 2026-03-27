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

import pendulum

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import dag
from kubernetes.client import models as k8s

KUBE_CONN_ID  = "kubernetes_default"
DBT_NAMESPACE = "data-warehouse"
DBT_IMAGE     = "drivexdocker/atlas-prod:21-3419b8c"


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
    Spins up a fresh pod with the dbt image, runs `dbt run`, then tears it down.
    Credentials are injected from the `atlas-dbt-env` Kubernetes Secret.
    """
    KubernetesPodOperator(
        task_id="run_dbt",
        name="atlas-dbt-run",
        namespace=DBT_NAMESPACE,
        image=DBT_IMAGE,
        cmds=["dbt", "run"],
        kubernetes_conn_id=KUBE_CONN_ID,
        image_pull_secrets=[
            k8s.V1LocalObjectReference(name="dockerhub-creds")
        ],
        env_from=[
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(name="atlas-dbt-prod-env")
            )
        ],
        volumes=[
            k8s.V1Volume(
                name="dbt-profiles",
                config_map=k8s.V1ConfigMapVolumeSource(name="atlas-dbt-profiles-prod"),
            )
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name="dbt-profiles",
                mount_path="/root/.dbt",
                read_only=True,
            )
        ],
        is_delete_operator_pod=True,  # clean up pod after run
        get_logs=True,
        in_cluster=True,
        startup_timeout_seconds=300,
    )


atlas_warehouse_sync()
