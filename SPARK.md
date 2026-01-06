## How to submit a Spark job without airflow

### Step 1: Upload the file to Minio

Port-forward Minio console to access the console

```bash
kubectl port-forward svc/minio-console 9090:9090 -n airflow
```

The Minio container should already have the `spark-scripts` bucket created, if not create it yourself. Upload both the Spark script and the data to the `spark-scripts` bucket.

### Step 2: Edit the spark operator yaml file

Edit the id of the job, optionally to a unique name

```yaml
metadata:
  # Change this
  name: minio-test-job
```

Edit the `mainApplicationFile` field to match the name of the script

```yaml
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: "frostedpilot/spark:3.5.4"
  sparkVersion: "3.5.4"

  # Edit this field here
  mainApplicationFile: "s3a://spark-scripts/read_minio.py"
```

Optionally edit the driver and worker specs to suit the job requirement too

```yaml
executor:
  # Increase number of cores, instances and memory based on need
  cores: 1
  instances: 1
  memory: "512m"
  volumeMounts:
    - name: ivy-cache
      mountPath: /tmp
```

### Step 3: Submit the job

```bash
k3s kubectl delete sparkapplication <<job_id_in_yaml_file>> -n default --ignore-not-found
k3s kubectl apply -f sample_spark_operator_file.yaml
```

Wait for the container to be created, then check the log

```bash
k3s kubectl logs -f -n default <<job_id_in_yaml_file>>_driver
```
