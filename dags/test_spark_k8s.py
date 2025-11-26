from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="test_spark_k8s",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["spark", "test"],
) as dag:

    # Define the Spark Application
    spark_app_yaml = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": "spark-test-{{ ts_nodash|lower }}",
            "namespace": "airflow",
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": "docker.io/bitnamilegacy/spark:3.5.5",
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": "local:///opt/spark/scripts/job.py",  # Path inside the pod
            "sparkVersion": "3.5.5",
            "restartPolicy": {"type": "Never"},
            "driver": {
                "cores": 1,
                "memory": "512m",
                "serviceAccount": "spark-sa",  # Make sure you created this SA in previous steps!
                "volumeMounts": [
                    {"name": "scripts-vol", "mountPath": "/opt/spark/scripts"}
                ],
            },
            "executor": {
                "cores": 1,
                "instances": 1,
                "memory": "512m",
                "volumeMounts": [
                    {"name": "scripts-vol", "mountPath": "/opt/spark/scripts"}
                ],
            },
            "volumes": [
                {
                    "name": "scripts-vol",
                    "configMap": {
                        "name": "spark-scripts"  # Matches the ConfigMap name
                    },
                }
            ],
        },
    }

    submit = SparkKubernetesOperator(
        task_id="submit_spark_job",
        namespace="airflow",
        application_file=spark_app_yaml,
        kubernetes_conn_id="kubernetes_default",
    )

    sensor = SparkKubernetesSensor(
        task_id="monitor_spark_job",
        namespace="airflow",
        application_name="spark-test-{{ ts_nodash|lower }}",
        kubernetes_conn_id="kubernetes_default",
    )

    submit >> sensor
