# Deployment guide

## Local Docker Compose

Copy `.env.example` to `.env`, then run `docker compose up --build -d`. A one-shot init service creates all topics before the pipeline starts. The first Spark submission downloads the Kafka connector and therefore needs internet access. The host `data/` directory contains checkpoints, Parquet datasets, and control-plane metadata. Kafka and Redis use named volumes.

The Spark UI is at `localhost:8080` and the incident API at `localhost:8000/docs`. Produce a demo batch with `docker compose --profile tools run --rm producer`. Stop services with `docker compose down`; add `-v` only when intentionally discarding broker and Redis data.

## Kubernetes

Build and push an immutable pipeline image, replace both example image names, and replace the Kafka `ExternalName`. Create `pipeline-secrets` through a secret manager. Apply namespace, ConfigMap, storage/Redis, Kafka reference, pipeline, API, and HPA in that order.

The example uses a single-writer PVC and one pipeline replica because local Parquet/checkpoint storage is not safe for concurrent writers. Production should mount object storage through native Spark connectors or use an ACID table format. Use a managed Kafka and Redis service, network policies, PodDisruptionBudgets, workload identity, and Prometheus alerts. For distributed Spark execution, use the Spark Operator rather than the readable standalone example deployment.

## Local non-container Spark

Install Java 17 and dependencies. The Kafka connector must match Spark/Scala:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  main.py pipeline
```
