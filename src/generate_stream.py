"""Generate a synthetic Kafka-style order-event stream.

Events are written in RECEIVED order (the order a consumer would see
them), not event order: duplicates are re-delivered, some events arrive
late, and a few are malformed. Seeded, so re-running reproduces the
file byte-for-byte.

Event schema: event_id, order_id, user_id, event_type
(purchase|refund), amount, currency, event_ts, received_ts, channel.
"""
import argparse, json, random
from datetime import datetime, timedelta, timezone

def ts(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=20261103)
    ap.add_argument("--n", type=int, default=5000)
    ap.add_argument("--out", default="data/raw_stream.jsonl")
    a = ap.parse_args()
    rng = random.Random(a.seed)
    start = datetime(2026, 6, 1, tzinfo=timezone.utc)
    events = []
    for i in range(a.n):
        # Event time spread over 3 days, denser in business hours.
        day = rng.randrange(3)
        hour = rng.choice([9,10,11,12,13,14,15,16,17,18,19,20] + list(range(24)))
        minute = rng.randrange(60); second = rng.randrange(60)
        et = start + timedelta(days=day, hours=hour, minutes=minute, seconds=second)
        etype = "refund" if rng.random() < 0.08 else "purchase"
        amount = round(rng.uniform(10, 500), 2)
        if etype == "refund":
            amount = -abs(amount)
        ev = {"event_id": f"evt-{i:05d}", "order_id": f"ord-{rng.randrange(2500):04d}",
              "user_id": f"usr-{rng.randrange(1200):04d}", "event_type": etype,
              "amount": amount, "currency": "USD",
              "event_ts": ts(et), "channel": rng.choice(["web", "mobile", "api"])}
        # Received time: usually seconds-minutes later; ~4% arrive hours late.
        if rng.random() < 0.04:
            delay = timedelta(hours=rng.uniform(3, 30))
        else:
            delay = timedelta(seconds=rng.randrange(5, 1800))
        ev["received_ts"] = ts(et + delay)
        events.append(ev)
    # Duplicate deliveries: 3% of events re-delivered later, same event_id.
    dups = []
    for ev in rng.sample(events, int(a.n * 0.03)):
        d = dict(ev)
        d["received_ts"] = ts(datetime.strptime(ev["received_ts"], "%Y-%m-%dT%H:%M:%SZ") + timedelta(minutes=rng.randrange(10, 240)))
        dups.append(d)
    events += dups
    # Malformed rows: 60 total (20 null amount, 20 negative purchase, 20 unknown type).
    for j in range(20):
        bad = dict(rng.choice(events)); bad["event_id"] = f"bad-null-{j:02d}"; bad["amount"] = None; events.append(bad)
    for j in range(20):
        bad = dict(rng.choice(events)); bad["event_id"] = f"bad-neg-{j:02d}"; bad["event_type"] = "purchase"; bad["amount"] = -abs(bad["amount"]); events.append(bad)
    for j in range(20):
        bad = dict(rng.choice(events)); bad["event_id"] = f"bad-type-{j:02d}"; bad["event_type"] = "mystery"; events.append(bad)
    events.sort(key=lambda e: e["received_ts"])
    with open(a.out, "w") as f:
        for ev in events:
            f.write(json.dumps(ev, sort_keys=True) + "\n")
    print(f"wrote {len(events)} events to {a.out}")

if __name__ == "__main__":
    main()
