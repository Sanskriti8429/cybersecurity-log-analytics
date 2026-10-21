import random
from datetime import datetime, timedelta

from faker import Faker

fake= Faker()

HOST= "server01"
USERS= ["admin", "root", "ubuntu","deploy","Priya", "Raj", "Payal", "Rohan"]
ATTACK_TARGETS= ["admin", "root", "ubuntu"]

def auth_line(ts, success, user, ip, port):
    stamp=  f"{ts:%b} {ts.day:>2} {ts:%H:%M:%S}"
    pid= random.randint(1000,9999)
    outcome= "Accepted" if success else "Failed"
    return f"{stamp} {HOST} sshd[{pid}]: {outcome} password for {user} from {ip} port {port} ssh2"

def normal_event(ts):
    success= random.random() <0.95
    line= auth_line(ts, success, random.choice(USERS), fake.ipv4_private(), random.randint(1024, 65535))
    return(ts, line, "normal")

def brute_force_burst(start_ts):
    ip= fake.ipv4_public()
    targets= random.sample(ATTACK_TARGETS, k=2)
    ts= start_ts
    events= []
    for _ in range(random.randint(30,200)):
        ts += timedelta(milliseconds=random.randint(100,1500))
        line= auth_line(ts, False, random.choice(targets), ip, random.randint(1024, 65535))
        events.append((ts, line, "brute_force"))
    return events

def noisy_legit_burst(start_ts):
    ip = fake.ipv4_private()
    user = random.choice(USERS)
    ts = start_ts
    events = []
    for _ in range(random.randint(15, 40)):
        ts += timedelta(seconds=random.uniform(2, 5))
        line = auth_line(ts, False, user, ip, random.randint(1024, 65535))
        events.append((ts, line, "normal"))
    return events


def slow_brute_force(start_ts):
    ip = fake.ipv4_public()
    ts = start_ts
    events = []
    for _ in range(random.randint(30, 60)):
        ts += timedelta(seconds=random.uniform(8, 15))
        line = auth_line(ts, False, random.choice(ATTACK_TARGETS), ip,
                         random.randint(1024, 65535))
        events.append((ts, line, "slow_brute_force"))
    return events


def distributed_brute_force(start_ts):
    target = random.choice(ATTACK_TARGETS)
    events = []
    for _ in range(random.randint(15, 25)):
        ip = fake.ipv4_public()
        ts = start_ts + timedelta(seconds=random.uniform(0, 30))
        for _ in range(random.randint(4, 7)):
            ts += timedelta(seconds=random.uniform(8, 15))
            line = auth_line(ts, False, target, ip, random.randint(1024, 65535))
            events.append((ts, line, "distributed_brute_force"))
    return events


def generate(n_normal, n_bursts, n_noisy=2, n_slow=2, n_distributed=2,
             start=None, window_seconds=600):
    start = start or datetime.now()
    events = []
    for _ in range(n_normal):
        ts = start + timedelta(seconds=random.uniform(0, window_seconds))
        events.append(normal_event(ts))
    scenarios = [
        (n_bursts, brute_force_burst),
        (n_noisy, noisy_legit_burst),
        (n_slow, slow_brute_force),
        (n_distributed, distributed_brute_force),
    ]
    for count, scenario in scenarios:
        for _ in range(count):
            events.extend(scenario(start + timedelta(seconds=random.uniform(0, window_seconds))))
    events.sort(key=lambda e: e[0])
    return events


if __name__ == "__main__":
    from collections import Counter

    events = generate(n_normal=1000, n_bursts=5)
    print(Counter(label for _, _, label in events))
    print(f"Total events: {len(events)}")