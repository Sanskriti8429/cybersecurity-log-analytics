import json
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27018")
docs = list(client["security_logs"]["alerts"].find())
for d in docs:
    d["_id"] = str(d["_id"])

with open("dashboard/alerts.json", "w") as f:
    json.dump(docs, f, indent=2)

print(f"exported {len(docs)} alerts to dashboard/alerts.json")