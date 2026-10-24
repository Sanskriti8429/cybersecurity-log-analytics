from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://mongodb:27017")
coll = client["security_logs"]["alerts"]

latencies = []
for doc in coll.find({"last_kafka_ts": {"$exists": True}}):
    last_kafka_ts = datetime.fromisoformat(doc["last_kafka_ts"])
    detected_at = datetime.fromisoformat(doc["detected_at"])
    latency_sec = (detected_at - last_kafka_ts).total_seconds()
    latencies.append(latency_sec)

latencies.sort()
n = len(latencies)
if n == 0:
    print("no alerts with last_kafka_ts found — rerun the producer with the updated streaming job first")
else:
    print(f"streaming alerts measured: {n}")
    print(f"min latency:    {latencies[0]:.2f}s")
    print(f"median latency: {latencies[n // 2]:.2f}s")
    print(f"max latency:    {latencies[-1]:.2f}s")
    print(f"mean latency:   {sum(latencies) / n:.2f}s")