import uuid
from confluent_kafka import Producer
from log_generator import generate

producer= Producer({"bootstrap.servers": "localhost:9092"})

def send(events):
    for ts, line, label in events:
        event_id= str(uuid.uuid4())
        producer.produce("auth-logs", key= event_id, value=line)
        producer.produce("ground-truth", key=event_id, value=label)
        producer.poll(0)
    producer.flush()
    
if __name__ == "__main__":
    events= generate(n_normal=1000, n_bursts=5)
    send(events)
    print(f"Sent {len(events)} events")