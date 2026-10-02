
import json, math, random, time, uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker

TOPIC = "transactions"
N_USERS = 200
RATE_PER_SEC = 10         
FRAUD_RATE = 0.03       
WARMUP_SECONDS = 30        

CITIES = {
    "Kolkata": (22.57, 88.36), "Mumbai": (19.08, 72.88), "London": (51.51, -0.13),
    "New York": (40.71, -74.01), "Singapore": (1.35, 103.82), "Dubai": (25.20, 55.27),
}
MERCHANTS = [f"merchant_{i:03d}" for i in range(60)]
RING_DEVICES = ["dev_ring_a", "dev_ring_b", "dev_ring_c"]

fake = Faker()
producer = Producer({"bootstrap.servers": "localhost:9092"})

users = {}
for i in range(N_USERS):
    users[f"user_{i:04d}"] = {
        "home": random.choice(list(CITIES)),
        "avg": random.uniform(20, 300),
        "devices": [f"dev_{uuid.uuid4().hex[:8]}" for _ in range(random.choice([1, 2]))],
        "merchants": random.sample(MERCHANTS, 6),
    }


def make_tx(uid, amount=None, city=None, device=None, merchant=None, label=False):
    u = users[uid]
    city = city or u["home"]
    lat, lon = CITIES[city]
    return {
        "txn_id": uuid.uuid4().hex,
        "user_id": uid,
        "amount": round(amount if amount is not None else random.lognormvariate(math.log(u["avg"]), 0.3), 2),
        "merchant": merchant or random.choice(u["merchants"]),
        "city": city,
        "lat": lat + random.uniform(-0.05, 0.05),
        "lon": lon + random.uniform(-0.05, 0.05),
        "device_id": device or random.choice(u["devices"]),
        "ts": datetime.now(timezone.utc).isoformat(),
        "label": label,
    }


def send(tx):
    producer.produce(TOPIC, key=tx["user_id"], value=json.dumps(tx))
    producer.poll(0)


def fraud_event(uid):
    u = users[uid]
    kind = random.choice(["spike", "travel", "burst", "ring"])
    print(f"[fraud injected] {kind} on {uid}")
    if kind == "spike":      
        send(make_tx(uid, amount=u["avg"] * random.uniform(8, 15),
                     merchant=random.choice(MERCHANTS), label=True))
    elif kind == "travel":   
        far = random.choice([c for c in CITIES if c != u["home"]])
        send(make_tx(uid, amount=u["avg"] * random.uniform(3, 6), city=far, label=True))
    elif kind == "burst":    
        for _ in range(7):
            send(make_tx(uid, amount=random.uniform(1, 10), label=True))
            time.sleep(0.2)
    elif kind == "ring":    
        dev = random.choice(RING_DEVICES)
        for victim in random.sample(list(users), 4):
            send(make_tx(victim, device=dev, label=True))
            time.sleep(0.2)


if __name__ == "__main__":
    start = time.time()
    print("Producing transactions. Ctrl+C to stop.")
    try:
        while True:
            uid = random.choice(list(users))
            if time.time() - start > WARMUP_SECONDS and random.random() < FRAUD_RATE:
                fraud_event(uid)
            else:
                send(make_tx(uid))
            time.sleep(1 / RATE_PER_SEC)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
