# Spark Job Submission Guide

This guide explains how to submit Spark jobs to the Kubernetes cluster without using Airflow.

---

## Overview

The cluster uses the **Spark Operator** to manage SparkApplication resources. Jobs are defined as YAML manifests and submitted via `kubectl`. Scripts and data are stored in **MinIO** (S3-compatible storage) for ease of usage. After scripts are fully tested, they should be baked into the Spark image.

---

## Step 1: Upload Files to MinIO

First, port-forward the MinIO console to access it locally:

```bash
kubectl port-forward svc/minio-console 9090:9090 -n airflow
```

Then open [http://localhost:9090](http://localhost:9090) in your browser.

> **Note:** The `spark-scripts` bucket should already exist. If not, create it manually.

Upload your files:
- **Spark script** (`.py` file)
- **Data files** (if needed)

---

## Step 2: Configure the SparkApplication YAML

Copy the sample file and modify it for your job:

```bash
cp deploy/spark/sample_spark_operator_file.yaml deploy/spark/my-job.yaml
```

### Required Changes

**1. Update the job name** (must be unique):

```yaml
metadata:
  name: my-spark-job  # Change this to a unique name
  namespace: default
```

**2. Set the main application file** (path in MinIO):

```yaml
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: "frostedpilot/spark:3.5.4"
  sparkVersion: "3.5.4"

  mainApplicationFile: "s3a://spark-scripts/your-script.py"  # Update this
```

**3. Adjust resources** (optional):

```yaml
driver:
  cores: 1
  memory: "512m"

executor:
  cores: 2        # Increase for heavier jobs
  instances: 2    # Number of executor pods
  memory: "1g"    # Memory per executor
```

---

## Step 3: Submit the Job

Delete any previous run (if exists) and apply the new job:

```bash
# Delete previous run (if any)
kubectl delete sparkapplication my-spark-job -n default --ignore-not-found

# Submit the job
kubectl apply -f deploy/spark/my-job.yaml
```

---

## Step 4: Monitor the Job

### Check job status

```bash
kubectl get sparkapplications -n default
```

### View driver logs

```bash
kubectl logs -f my-spark-job-driver -n default
```

### Check executor pods

```bash
kubectl get pods -n default -l spark-role=executor
```

---

## Available Spark Jobs

| Job | YAML File | Description |
|-----|-----------|-------------|
| Game Catalog | `deploy/spark/game.yaml` | Raw → Silver: Steam game metadata |
| Reviews | `deploy/spark/review.yaml` | Raw → Silver: Game reviews |
| Player History | `deploy/spark/history.yaml` | Raw → Silver: Player counts |
| Gold Analytics | `deploy/spark/gold.yaml` | Silver → Gold: Aggregations |
| DQ - Games | `deploy/spark/dq_game.yaml` | Data quality checks on games |
| DQ - Reviews | `deploy/spark/dq_review.yaml` | Data quality checks on reviews |

### Quick Submit Examples

```bash
# Run game catalog processing
kubectl apply -f deploy/spark/game.yaml

# Run gold analytics
kubectl apply -f deploy/spark/gold.yaml

# Run data quality checks
kubectl apply -f deploy/spark/dq_game.yaml
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Pod stuck in `Pending` | Check node resources: `kubectl describe pod <pod-name>` |
| Driver crash | View logs: `kubectl logs <job>-driver --previous` |
| S3 access error | Verify MinIO credentials in `hadoopConf` section |
| Job not starting | Check Spark Operator logs: `kubectl logs -n spark-operator -l app.kubernetes.io/name=spark-operator` |
