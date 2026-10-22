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

FW_HOST = "server01"
SERVER_IP = "10.0.0.5"
COMMON_PORTS = [22, 80, 443, 3306, 8080]


def fw_line(ts, action, src, dport, sport):
    """Build one UFW-style firewall line."""
    stamp = f"{ts:%b} {ts.day:>2} {ts:%H:%M:%S}"
    return (
        f"{stamp} {FW_HOST} kernel: [UFW {action}] IN=eth0 OUT= SRC={src} DST={SERVER_IP} "
        f"LEN=60 TTL={random.randint(40, 64)} PROTO=TCP SPT={sport} DPT={dport} SYN"
    )


def normal_fw_event(ts):
    """Normal traffic: internal hosts reach allowed services, plus rare stray blocked packets."""
    if random.random() < 0.97:
        line = fw_line(ts, "ALLOW", fake.ipv4_private(),
                       random.choice([22, 80, 443]), random.randint(1024, 65535))
    else:
        line = fw_line(ts, "BLOCK", fake.ipv4_public(),
                       random.choice(COMMON_PORTS), random.randint(1024, 65535))
    return (ts, line, "normal")


def port_scan(start_ts):
    """One attacker IP probing many different ports within seconds."""
    ip = fake.ipv4_public()
    ts = start_ts
    events = []
    for dport in random.sample(range(1, 10000), random.randint(100, 500)):
        ts += timedelta(milliseconds=random.randint(5, 100))
        line = fw_line(ts, "BLOCK", ip, dport, random.randint(1024, 65535))
        events.append((ts, line, "port_scan"))
    return events


def generate_firewall(n_normal, n_scans, start=None, window_seconds=600):
    """Return (timestamp, line, label) firewall events sorted by time."""
    start = start or datetime.now()
    events = [
        normal_fw_event(start + timedelta(seconds=random.uniform(0, window_seconds)))
        for _ in range(n_normal)
    ]
    for _ in range(n_scans):
        events.extend(port_scan(start + timedelta(seconds=random.uniform(0, window_seconds))))
    events.sort(key=lambda e: e[0])
    return events

def _seed(seed):
    if seed is not None:
        random.seed(seed)
        Faker.seed(seed)

if __name__ == "__main__":
    from collections import Counter

    events = generate(n_normal=1000, n_bursts=5)
    print(Counter(label for _, _, label in events))
    print(f"Total events: {len(events)}")