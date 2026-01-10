# Big Data Pipeline for Steam Game Analysis

This project implements a distributed pipeline for scraping, processing, and analyzing game data from Steam and SteamDB. The project use Kubernetes (specifically k3s) running on a cluster of 3 to 5 laptops connected via a virtual private network.

Note: the new data is located at [data](https://www.mediafire.com/file/4hik88ybarvtekf/new_new_data.rar/file)

## Architecture

The architecture uses Airflow as the central orchestrator, scheduling tasks that leverage Kafka for data streaming, Spark for distributed processing, and HDFS and MongoDB for storage.

```mermaid
---
config:
  layout: fixed
  theme: default
---
flowchart TD
    subgraph A["External Data Sources"]
        Sources("SteamDB & Steam API")
    end

    subgraph B["Kubernetes Cluster"]
        direction LR
        subgraph B1["Ingestion & Orchestration"]
            Airflow["Airflow"]:::orchestration
            Scraper["Python Scraper"]:::ingestion
            Kafka["Kafka"]:::storage
        end

        subgraph B2["Processing & Storage"]
            Spark["Spark"]:::processing
            HDFS["HDFS Data Lake<br>(Raw→Silver→Gold)"]:::storage
            DQOps["DQOps"]:::processing
        end

        subgraph B3["Serving Layer"]
            MongoDB["MongoDB"]:::storage
            Trino["Trino SQL Engine"]:::processing
            Feast["Feast Feature Store"]:::ml
        end
    end

    subgraph C["Downstream Consumers"]
        Consumers["BI, Analytics & ML Applications"]
    end

    %% --- Main Data Flow ---
    Sources -- Fetched Data --> Scraper -- Raw Data --> Kafka -- Data Stream --> Spark
    Spark -- Processes & Stores Data --> HDFS
    DQOps -- Monitors Data Quality --> HDFS
    Airflow -- Schedules Pipeline --> Spark

    %% --- Three Paths to Consumers ---
    Spark -- Aggregated Data --> MongoDB
    MongoDB -- <b>For Dashboards (Power BI)</b> --> Consumers

    HDFS -- Granular Data --> Trino
    Trino -- <b>For Ad-Hoc SQL Analytics</b> --> Consumers

    HDFS -- Historical Features --> Feast
    Feast -- <b>For Real-time ML Models</b> --> Consumers


    %% --- Styling ---
    classDef storage fill:#D5E8D4,stroke:#82B366,stroke-width:2px
    classDef processing fill:#F8CECC,stroke:#B85450,stroke-width:2px
    classDef orchestration fill:#DAE8FC,stroke:#6C8EBF,stroke-width:2px
    classDef ingestion fill:#FFE6CC,stroke:#D79B00,stroke-width:2px
    classDef ml fill:#F5F5DC,stroke:#BDB76B,stroke-width:2px
```

## Tech Stack

- **Operating System**: Ubuntu-22.04 on WSL2 on Windows
- **Container Runtime**: `containerd` (packaged with k3s)
- **Networking**: Tailscale (for static IP for each laptop)
- **Kubernetes Distribution**: k3s
- **Application Deployment**: Helm
- **Orchestrator**: Apache Airflow
- **Processing Engine**: Apache Spark
- **Streaming/Messaging**: Apache Kafka
- **Distributed Storage**: HDFS
- **NoSQL Database**: MongoDB
- **Query Engine**: Trino
- **Data Quality**: DQOps
- **Feature Store**: Feast

---

## Starting guide for project members

This guide details the steps to create an n-node Kubernetes cluster across n laptops and deploy the big data stack. One laptop will be selected as the K3s server node, and the other laptops as agent nodes.

### Step 1: Prerequisites (All Laptops)

1.  **Install WSL2**: Follow the guide on the official Microsoft page

2.  **Install Tailscale**:

    - **On WSL2 (Ubuntu Terminal)**: Run the following to install and start Tailscale.

      ```bash
      curl -fsSL https://tailscale.com/install.sh | sh
      sudo tailscale up

      # On first install, there will be a link. Send it to Discord so I can add you to Tailnet
      ```

3.  **Share IP Addresses**: Each member must find their Tailscale IP address and share it with the team.
    ```bash
    tailscale ip -4
    # Share the ip on Discord
    ```

### Step 2: Cluster Formation (k3s)

Designate one member as the **Server Node** and the others as **Agent Nodes**.

#### On the Server Node ONLY

Run these commands in your WSL2 terminal.

0. Get the token

```
openssl rand -hex 16
# Copy the result
```

1. Edit the .bashrc file

```
nano ~/.bashrc
# or
vim ~/.bashrc
```

2. Paste the following into the .bashrc file

```bash
export K3S_TOKEN="THE HEX RESULT COPIED IN STEP 0"
export SERVER_IP=$(tailscale ip -4)
export TAILSCALE_AUTH_KEY="THE TAILSCALE AUTH KEY"
```

3. Install k3s server with permissions for the config file

```
source ~/.bashrc
export INSTALL_K3S_EXEC="server --token=${K3S_TOKEN} --vpn-auth=name=tailscale,joinKey=${TAILSCALE_AUTH_KEY} --node-external-ip=${SERVER_IP}"
curl -sfL https://get.k3s.io | sh -
```

After k3s is install:

```bash
# 4. Configure kubectl for your user
mkdir -p ~/.kube
cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sed -i "s/127.0.0.1/${SERVER_IP}/" ~/.kube/config
echo "kubectl is now configured."
```

#### On the 4 Agent Nodes ONLY

Each of the other four students must run these commands in their WSL2 terminal.

1. Edit the .bashrc

```
nano ~/.bashrc
# or
vim ~/.bashrc
```

2. Paste the env variables got from the server node into .bashrc

```bash
# 1. Set environment variables
#  Get the env from Discord!!!
export K3S_TOKEN="<PASTE_THE_SHARED_TOKEN_HERE>"
export SERVER_IP="<PASTE_SERVER_TAILSCALE_IP_HERE>"
export TAILSCALE_AUTH_KEY="<PASTE_TAILSCALE_AUTH_KEY_HERE>"
export K3S_URL="https://${SERVER_IP}:6443"
export AGENT_IP=$(tailscale ip -4)
```

3. Install k3s agent

```
export INSTALL_K3S_EXEC="agent --vpn-auth=name=tailscale,joinKey=${TAILSCALE_AUTH_KEY} --node-external-ip=${AGENT_IP} --server=${K3S_URL} --token=${K3S_TOKEN}"
curl -sfL https://get.k3s.io | sh -
```

4. Configure the k3s agent to be able to use kubectl

Download the `k3s.yaml` file on Discord and put it into the `~/.kube/` folder as the file `config`:

```bash
mkdir -p $HOME/.kube
cp k3s.yaml ~/.kube/config
```

### Step 3: Configuration Files

Clone the repo, it should contain all config files (yaml files)

### Step 4: Deploying the Application Stack

On the **Server Node**, run these commands to install everything.

Install Helm

```bash
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod 700 get_helm.sh
./get_helm.sh
```

Add necessary helm repos

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add airflow-community https://airflow-helm.github.io/charts
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo add pfisterer https://pfisterer.github.io/apache-hadoop-helm/
helm repo add nessie-helm https://charts.projectnessie.org
helm repo add strimzi https://strimzi.io/charts/
helm repo update
```

Deploy the applications

```bash
helm install my-mongodb bitnami/mongodb --namespace database --create-namespace -f deploy/helm/mongodb-values.yaml
helm install airflow airflow-community/airflow --namespace airflow -f deploy/helm/airflow-values.yaml
helm install spark-operator spark-operator/spark-operator --namespace spark-operator --set sparkJobNamespace="" --set webhook.enable=true
helm install my-hadoop pfisterer/hadoop   --namespace hadoop   -f deploy/helm/hdfs_values.yaml

# Trino stuff
helm install nessie nessie-helm/nessie --namespace nessie-ns --create-namespace -f deploy/helm/nessie-values.yaml
kubectl create configmap trino-hadoop-conf --namespace trino --from-file=core-site.xml=./core-site.xml --from-file=hdfs-site.xml=./hdfs-site.xml
helm install my-trino trino/trino --namespace trino --create-namespace -f deploy/helm/trino-values.yaml

# Provide the same function as hadoop, but easier to use, for dev purpose only
helm install minio bitnami/minio   --namespace airflow   --values deploy/helm/minio_values.yaml

# Kafka (Strimzi Operator + KRaft Cluster)
helm install strimzi-kafka-operator strimzi/strimzi-kafka-operator --namespace strimzi --create-namespace
# Wait for operator to start, then apply cluster and topics:
kubectl apply -f deploy/kafka/kafka-cluster.yaml -n kafka
kubectl apply -f deploy/kafka/kafka-topics.yaml -n kafka

# Redis (State Store) - Using Legacy Image for stability
helm install redis bitnami/redis \
  --namespace redis \
  --create-namespace \
  --set image.repository=bitnamilegacy/redis \
  --set image.tag=8.2-debian-12 \
  --set architecture=standalone

helm install airflow airflow-community/airflow --namespace airflow -f deploy/helm/airflow-values.yaml

```

### Step 5: Secrets & Data Bootstrap

Before deploying producers, we must secure credentials and populate the Kafka log with historical data.

**1. Create Kubernetes Secrets**
```bash
# 1. Get the auto-generated Redis password
export REDIS_PASS=$(kubectl get secret --namespace redis redis -o jsonpath="{.data.redis-password}" | base64 -d)

# 2. Create the Steam API Key Secret (Replace with your key)
kubectl create secret generic steam-api-secrets --from-literal=api-key=YOUR_STEAM_API_KEY

# 3. Create the Manual Redis Secret (Required for producers to find the password)
kubectl create secret generic redis-secret --from-literal=password=$REDIS_PASS
```

**2. Bootstrap Historical Data (Run Locally)**
Perform these steps from the root of the project on your local machine.

*   **Get Connection Info:**
    ```bash
    NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[0].address}')
    NODE_PORT=$(kubectl get svc my-kafka-cluster-kafka-external-bootstrap -n kafka -o jsonpath='{.spec.ports[0].nodePort}')
    echo "Update your python scripts KAFKA_BOOTSTRAP_SERVERS with: $NODE_IP:$NODE_PORT"
    ```

*   **Run Bootstrap Scripts:**
    1.  Open a tunnel to Redis: `kubectl port-forward redis-master-0 6379:6379 -n redis`
    2.  Run Redis Bootstrap: `python scripts/bootstrap/bootstrap_redis.py`
    3.  Run Kafka Backfill: `python scripts/bootstrap/bootstrap_data.py` (Ensure you updated the IP:PORT inside the script).

---

### Step 6: Deploying the Ingestion Fleet (Kappa Producers)

We deploy Dockerized Python producers to continuously ingest live data.

**1. Build & Push Images**
Navigate to each producer folder (`src/producers/player_count`, `src/producers/live_review`, `src/producers/game_catalog`), build the Docker image, and push it to your registry.

This step can be skipped as the images are already built.

**2. Apply Deployments**
Deploy the long-running services to the cluster using the YAML files in the deploy/producers directory.

```bash
kubectl apply -f deploy/producers/deploy-player-count-producer.yaml
kubectl apply -f deploy/producers/deploy-live-review-producer.yaml
kubectl apply -f deploy/producers/deploy-game-catalog-producer.yaml
```

**3. Verification**
Check that producers are running and logging data:

```bash
# Check status
kubectl get pods

# Check logs (example)
kubectl logs -f -l app=player-count-producer
```


### Side notes

How to use Spark: [Spark guide](SPARK.md)

### Some useful commands

1. Get all nodes

```bash
k3s kubectl get nodes -o wide
```

2. Get all pods

```bash
k3s kubectl get pods -o wide -n <namespace>
```

3. Get detailed node information

```bash
k3s kubectl describe nodes
```

4. Get pod details

```bash
kubectl describe pod <pod-name> -n <namespace>
```

5. Get pod logs

```bash
kubectl logs -f <pod-name> -n <namespace>
```

6. Get previous pod logs (useful for debug/crash)

```bash
kubectl logs -f <pod-name> -n <namespace> --previous
```

7. Execute the command in the pod

```bash
kubectl exec -it <pod-name> -n <namespace> -- /bin/bash
```

8. Uninstall k3s

Server node:

```bash
/usr/local/bin/k3s-uninstall.sh
sudo rm -rf /etc/rancher/ /var/lib/rancher/
```

Agent node:

```bash
/usr/local/bin/k3s-agent-uninstall.sh
sudo rm -rf /etc/rancher/ /var/lib/rancher/
```


