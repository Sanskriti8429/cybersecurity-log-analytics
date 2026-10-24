## 1. Rule-Based Detection (Synthetic Data)

### Port Scan — Window Size Comparison

| Window | Slide | TP   | FP | FN  | Precision | Recall | Att TP | Att FN |
|--------|-------|------|----|-----|-----------|--------|--------|--------|
| 1 min  | 10s   | 2644 | 0  | 304 | 1.000     | 0.897  | 5      | 2      |
| 5 min  | 30s   | 2948 | 0  | 0   | 1.000     | 1.000  | 7      | 0      |

**Script:** `spark_jobs/eval_portscan.py`

### Brute Force — Combined Rules

Rules: A (per-IP 10/60s), B (per-IP 20/10min), C (per-user 20/60s from 5+ IPs).

| Rule               | TP  | FP | FN | Precision | Recall | F1    |
|--------------------|-----|----|----|-----------|--------|-------|
| A + B + C combined | 860 | 20 | 42 | 0.977     | 0.953  | 0.965 |

**Script:** `spark_jobs/evaluate_rules_v2.py`

---

## 2. Rule-Based Detection Validated Against Real Attacks (Kali Lab)

| Attack          | Tool                   | Rule                        | Result                                  |
|-----------------|------------------------|-----------------------------|-----------------------------------------|
| SSH brute force | Hydra (301 words)      | Rule A (10 fails/60s)       | Flagged in all 3 windows (71,98,10)     |
| SSH brute force | Hydra                  | Rule B (20 fails/10min)     | Flagged (179 fails in one window)       |
| Port scan       | Nmap -sT, ports 1-1000  | detect_port_scans (15/min)  | Flagged (1000 ports hit in <1s)         |

**Scripts:**
- `spark_jobs/eval_bruteforce_hydra.py`
- `spark_jobs/eval_portscan_nmap.py`

**Lab setup:** `lab/docker-compose.yml`

---

## 3. Port-Scan Threshold Sweep (CICIDS2017)

*Real scanner vs. 17,004 benign IPs.*

| Threshold | Scanner Caught | Benign IPs Flagged |
|-----------|----------------|--------------------|
| 15        | 1/1            | 447                |
| 50        | 1/1            | 55                 |
| 100       | 1/1            | 17                 |
| 200       | 1/1            | 7                  |
| 500       | 1/1            | 4                  |
| 1000      | 0/1            | 2                  |

- **Scanner peak:** 999 ports/min
- **Busiest benign host:** 1894 ports/min
- **Conclusion:** No threshold cleanly separates them.

**Script:** `spark_jobs/portscan_threshold_sweep.py`

---

## 4. ML-Based Detection (CICIDS2017)

*Multiclass Random Forest.*

| Model                 | Accuracy | Weighted F1 | Macro F1 |
|-----------------------|----------|-------------|----------|
| Baseline (unweighted) | 0.985    | 0.984       | 0.665    |
| Class-weighted        | 0.972    | 0.979       | 0.732    |

**Scripts:**
- `spark_jobs/prepare_ml_data.py`
- `spark_jobs/train_rf.py`

---

## 5. Rule-Based vs. ML Comparison

*Per-attacker-IP scoring.*

| Attack      | Method             | Precision | Recall |
|-------------|--------------------|-----------|--------|
| PortScan    | Rule (15/min)      | 0.005     | 1.000  |
| PortScan    | ML (weighted RF)   | 0.500     | 1.000  |
| SSH Patator | Rule (10/60s)       | 0.500     | 1.000  |
| SSH Patator | ML (weighted RF)   | 0.167     | 1.000  |
| FTP Patator | Rule (10/60s)       | 0.500     | 1.000  |
| FTP Patator | ML (weighted RF)   | 1.000     | 1.000  |

**Script:** `spark_jobs/compare_rules_vs_ml.py`

---

## 6. Batch vs. Streaming Latency

| Metric               | Batch   | Streaming    |
|----------------------|---------|--------------|
| Time to first alert  | 71.16s  | 5.40s (mean) |

- **Batch:** Full pipeline (produce → Kafka → HDFS → rule evaluation).
- **Streaming:** Kafka arrival → Mongo write, per window.

**Scripts:**
- `spark_jobs/stream_brute_force_mongo.py`
- `spark_jobs/measure_streaming_latency.py`
- `spark_jobs/evaluate_rules_v2.py`

---

## 7. Scalability Benchmark

*2.8M rows, rule-style aggregation.*

| Workers | Cores | Elapsed |
|---------|-------|---------|
| 1       | 2     | 9.55s   |
| 2       | 4     | 11.79s  |
| 3       | 6     | 16.12s  |

**Script:** `spark_jobs/benchmark_scalability.py`