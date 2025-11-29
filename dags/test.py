from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.utils.dates import days_ago
import uuid

# -------------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------------
IMAGE_NAME = "frostedpilot/spark:3.5.4"  # <--- REPLACE WITH YOUR IMAGE
SCRIPT_PATH = "s3a://spark-scripts/read-minio.py"
MINIO_ENDPOINT = "http://minio.airflow.svc:9000"


# Generate a unique job name every time the DAG runs
# Format: minio-read-job-abcd1234
def generate_job_name():
    return f"minio-read-job-{str(uuid.uuid4())[:8]}"


job_name = generate_job_name()

# The SparkApplication YAML (As a Python Dictionary)
spark_app_config = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {"name": job_name, "namespace": "default"},
    "spec": {
        "type": "Python",
        "mode": "cluster",
        "image": IMAGE_NAME,
        "imagePullPolicy": "IfNotPresent",
        "sparkVersion": "3.5.4",
        "mainApplicationFile": SCRIPT_PATH,
        "restartPolicy": {"type": "Never"},
        # MinIO Connection
        "hadoopConf": {
            "fs.s3a.endpoint": MINIO_ENDPOINT,
            "fs.s3a.access.key": "minioadmin",
            "fs.s3a.secret.key": "minioadmin",
            "fs.s3a.path.style.access": "true",
            "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "fs.s3a.connection.ssl.enabled": "false",
        },
        # Driver Settings
        "driver": {
            "cores": 1,
            "memory": "512m",
            "serviceAccount": "spark-operator-spark",
            "env": [{"name": "PYSPARK_PYTHON", "value": "/usr/bin/python3"}],
        },
        # Executor Settings
        "executor": {"instances": 1, "cores": 1, "memory": "512m"},
    },
}

# -------------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------------
with DAG(
    "spark_minio_test_v1",
    default_args={"owner": "airflow"},
    description="Submit Spark job to K3s via Spark Operator",
    schedule_interval=None,  # Trigger manually for testing
    start_date=days_ago(1),
    tags=["spark", "minio", "k3s"],
    catchup=False,
) as dag:

    # TASK 1: Submit the YAML to Kubernetes
    submit_job = SparkKubernetesOperator(
        task_id="submit_spark_job",
        namespace="default",
        application_file=spark_app_config,
        do_xcom_push=True,  # Saves the job details so the sensor can read them
    )

    # TASK 2: Watch the job until it finishes
    # This keeps the Airflow task "Running" until the Spark Pod succeeds
    monitor_job = SparkKubernetesSensor(
        task_id="monitor_spark_job",
        namespace="default",
        application_name=job_name,
        attach_log=True,  # Streams Spark logs into Airflow UI!
    )

    submit_job >> monitor_job
