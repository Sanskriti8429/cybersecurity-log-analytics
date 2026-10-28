# Cybersecurity Log Analytics

A distributed platform for ingesting, storing and analyzing large-scale security logs
to detect threats such as brute-force logins, port scans and anomalous access patterns,
built on Apache Spark.

## Overview

Security logs from login systems, firewalls, servers and applications grow too large
for single-machine analysis. This project builds an end-to-end Big Data pipeline that
ingests logs as a stream, stores them durably, detects attacks with both rule-based and
machine learning methods, and evaluates how well those approaches scale and perform.

## Tech Stack

- **Ingestion:** Apache Kafka
- **Storage:** Hadoop HDFS
- **Processing:** Apache Spark, PySpark, Spark MLlib
- **Results store:** MongoDB
- **Dashboard:** Streamlit, Plotly
- **Attack simulation:** Kali Linux (Nmap, Hydra)
- **Infrastructure:** Docker, Docker Compose
- **Language:** Python

## Repository Structure

```
docker/        Docker Compose files and service configuration
ingestion/     Log generators and Kafka producers
spark_jobs/    Batch, streaming and ML detection jobs
dashboard/     Streamlit dashboard
data/          Sample data (large files are git-ignored)
experiments/   Benchmarks and evaluation scripts
docs/          Design notes, literature review, diagrams
```

## Roadmap

### Foundation
- [x] Repository created
- [x] README, `.gitignore` and `requirements.txt`

### Infrastructure
- [x] MongoDB
- [x] Kafka
- [x] Spark cluster (master and workers)
- [x] HDFS (namenode and datanodes)
- [x] Full stack verified end to end

### Data
- [x] Synthetic log generator
- [x] Attack simulation with Kali tools
- [x] Public labeled datasets integrated
- [x] Kafka ingestion and HDFS storage

### Detection
- [x] Rule-based detection (batch)
- [x] Rule-based detection (streaming)
- [x] ML-based detection
- [x] Alert storage in MongoDB

### Evaluation
- [x] Scalability benchmarks
- [x] Batch vs streaming comparison
- [x] Rule-based vs ML comparison
- [x] Reproducible experiment result

### Dashboard
- [x] Live alerts and threat overview
- [x] Attack trends and top sources
