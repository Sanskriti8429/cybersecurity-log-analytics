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

def generate(n_normal, n_bursts, start= None, window_seconds=600):
    start= start or datetime.now()
    events=[]
    for _ in range(n_normal):
        ts= start+timedelta(seconds= random.uniform(0, window_seconds))
        events.append(normal_event(ts))
    for _ in range(n_bursts):
        burst_start= start + timedelta(seconds=random.uniform(0, window_seconds))
        events.extend(brute_force_burst(burst_start))
    events.sort(key= lambda e:e[0])
    return events

if __name__ == "__main__":
    events= generate(n_normal=20, n_bursts=1)
    for ts, line, label in events:
        print(f"[{label:11}] {line}")
    print(f"\nTotal events: {len(events)}")