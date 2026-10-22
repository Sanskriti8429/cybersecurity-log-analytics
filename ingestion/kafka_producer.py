import uuid

from kafka import KafkaProducer

from log_generator import generate, generate_firewall

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda s: s.encode("utf-8"),
    value_serializer=lambda s: s.encode("utf-8"),
)


def send(events, log_topic, truth_topic):
    """Send each event's raw line to log_topic and its label to truth_topic.

    Both messages share a unique id as the key, so they can be joined later.
    """
    for ts, line, label in events:
        event_id = str(uuid.uuid4())
        producer.send(log_topic, key=event_id, value=line)
        producer.send(truth_topic, key=event_id, value=label)
    producer.flush()


if __name__ == "__main__":
    auth = generate(n_normal=1000, n_bursts=5, seed=42)
    send(auth, "auth-logs", "ground-truth")
    print(f"Sent {len(auth)} auth events")

    firewall = generate_firewall(n_normal=1000, n_scans=5, seed=42)
    send(firewall, "firewall-logs", "firewall-truth")
    print(f"Sent {len(firewall)} firewall events")