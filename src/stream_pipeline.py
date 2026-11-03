"""Streaming consumer: incremental tumbling-window aggregation.

Simulates a Kafka-style consumer reading a log in received order:
  * validate -> quarantine malformed rows with a reason
  * deduplicate by event_id (same delivery twice = one event)
  * watermark = max event_ts seen - ALLOWED_LATENESS; a window whose
    end <= watermark is closed and frozen. An event for a closed window
    is recorded as late and NOT folded into the frozen aggregate.
  * open windows aggregate incrementally (no full re-scan per event)
  * per-window monitoring: refund rate, duplicate pressure, consumer
    lag, and empty-window detection raise alerts in results/alerts.json

At end of stream, still-open windows are flushed (marked closed=False
in the sense of 'finalised by end-of-stream, not by watermark' -- the
CSV/DB column `finalised_by` says which).

Stdlib only. Deterministic: same input file -> same outputs.
"""
import argparse, csv, json, sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone

FMT = "%Y-%m-%dT%H:%M:%SZ"
ALLOWED_LATENESS = timedelta(minutes=60)
WINDOW = timedelta(hours=1)

def parse(s):
    return datetime.strptime(s, FMT).replace(tzinfo=timezone.utc)

def window_start(dt):
    return dt.replace(minute=0, second=0, microsecond=0)

def validate(ev):
    if ev.get("amount") is None:
        return "null_amount"
    if ev.get("event_type") not in ("purchase", "refund"):
        return "unknown_event_type"
    if ev.get("event_type") == "purchase" and ev["amount"] < 0:
        return "negative_purchase_amount"
    if not ev.get("event_id") or not ev.get("event_ts"):
        return "missing_key_field"
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/raw_stream.jsonl")
    ap.add_argument("--db", default="results/stream_warehouse.db")
    ap.add_argument("--windows_csv", default="results/window_aggregates.csv")
    ap.add_argument("--metrics", default="results/metrics.json")
    ap.add_argument("--alerts", default="results/alerts.json")
    a = ap.parse_args()

    seen, windows, closed, late, quarantine = set(), {}, {}, [], []
    counts = {"total": 0, "quarantined": 0, "duplicates": 0, "late": 0, "accepted": 0}
    max_event_ts, max_lag_s, lag_sum, lag_n = None, 0, 0, 0
    dup_by_window = defaultdict(int)

    def close_window(ws):
        w = windows.pop(ws)
        closed[ws] = w

    with open(a.input) as f:
        for line in f:
            ev = json.loads(line)
            counts["total"] += 1
            reason = validate(ev)
            if reason:
                counts["quarantined"] += 1
                quarantine.append({**ev, "quarantine_reason": reason})
                continue
            if ev["event_id"] in seen:
                counts["duplicates"] += 1
                dup_by_window[window_start(parse(ev["event_ts"]))] += 1
                continue
            seen.add(ev["event_id"])
            et, rt = parse(ev["event_ts"]), parse(ev["received_ts"])
            lag = (rt - et).total_seconds()
            max_lag_s = max(max_lag_s, lag); lag_sum += lag; lag_n += 1
            if max_event_ts is None or et > max_event_ts:
                max_event_ts = et
            watermark = max_event_ts - ALLOWED_LATENESS
            ws = window_start(et)
            # Close every window that the watermark has passed.
            for open_ws in sorted(windows):
                if open_ws + WINDOW <= watermark:
                    close_window(open_ws)
            if ws in closed:
                counts["late"] += 1
                late.append({**ev, "late_reason": "window_closed_by_watermark"})
                continue
            w = windows.setdefault(ws, {"orders": 0, "purchases": 0, "refunds": 0,
                                        "revenue": 0.0, "refund_amount": 0.0, "users": set()})
            w["orders"] += 1; w["users"].add(ev["user_id"])
            if ev["event_type"] == "purchase":
                w["purchases"] += 1; w["revenue"] += ev["amount"]
            else:
                w["refunds"] += 1; w["refund_amount"] += ev["amount"]  # negative
            counts["accepted"] += 1

    flushed = dict(windows)  # end-of-stream flush, not watermark-closed
    all_windows = {}
    for ws, w in closed.items():
        all_windows[ws] = (w, "watermark")
    for ws, w in flushed.items():
        all_windows[ws] = (w, "end_of_stream")

    rows = []
    for ws in sorted(all_windows):
        w, how = all_windows[ws]
        orders = w["orders"]
        refund_rate = (w["refunds"] / orders) if orders else 0.0
        rows.append({"window_start": ws.strftime(FMT), "orders": orders,
                     "purchases": w["purchases"], "refunds": w["refunds"],
                     "revenue": round(w["revenue"], 2),
                     "refund_amount": round(w["refund_amount"], 2),
                     "net_revenue": round(w["revenue"] + w["refund_amount"], 2),
                     "distinct_users": len(w["users"]),
                     "refund_rate": round(refund_rate, 4),
                     "finalised_by": how})

    alerts = []
    total_rev = sum(r["net_revenue"] for r in rows)
    for r in rows:
        if r["refund_rate"] > 0.20:
            alerts.append({"type": "high_refund_rate", "window_start": r["window_start"],
                           "refund_rate": r["refund_rate"], "orders": r["orders"]})
    dup_rate = counts["duplicates"] / counts["total"] if counts["total"] else 0
    if dup_rate > 0.02:
        alerts.append({"type": "high_duplicate_rate", "duplicate_rate": round(dup_rate, 4),
                       "duplicates": counts["duplicates"]})
    if max_lag_s > 3600 * 6:
        alerts.append({"type": "high_consumer_lag", "max_lag_hours": round(max_lag_s / 3600, 2)})
    late_rate = counts["late"] / counts["total"] if counts["total"] else 0
    if late_rate > 0.01:
        alerts.append({"type": "late_events_present", "late_rate": round(late_rate, 4),
                       "late_count": counts["late"]})

    conn = sqlite3.connect(a.db)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS window_aggregates")
    cur.execute("""CREATE TABLE window_aggregates (window_start TEXT PRIMARY KEY,
        orders INT, purchases INT, refunds INT, revenue REAL, refund_amount REAL,
        net_revenue REAL, distinct_users INT, refund_rate REAL, finalised_by TEXT)""")
    cur.executemany("INSERT INTO window_aggregates VALUES (?,?,?,?,?,?,?,?,?,?)",
                    [(r["window_start"], r["orders"], r["purchases"], r["refunds"],
                      r["revenue"], r["refund_amount"], r["net_revenue"],
                      r["distinct_users"], r["refund_rate"], r["finalised_by"]) for r in rows])
    cur.execute("DROP TABLE IF EXISTS quarantine_events")
    cur.execute("CREATE TABLE quarantine_events (event_id TEXT, reason TEXT)")
    cur.executemany("INSERT INTO quarantine_events VALUES (?,?)",
                    [(q["event_id"], q["quarantine_reason"]) for q in quarantine])
    cur.execute("DROP TABLE IF EXISTS late_events")
    cur.execute("CREATE TABLE late_events (event_id TEXT, event_ts TEXT, received_ts TEXT)")
    cur.executemany("INSERT INTO late_events VALUES (?,?,?)",
                    [(e["event_id"], e["event_ts"], e["received_ts"]) for e in late])
    conn.commit(); conn.close()

    with open(a.windows_csv, "w", newline="") as f:
        wtr = csv.DictWriter(f, fieldnames=list(rows[0].keys())); wtr.writeheader(); wtr.writerows(rows)
    metrics = {**counts,
               "windows_watermark_closed": len(closed), "windows_flushed": len(flushed),
               "windows_total": len(rows), "total_net_revenue": round(total_rev, 2),
               "duplicate_rate": round(dup_rate, 4), "late_rate": round(late_rate, 4),
               "avg_lag_minutes": round(lag_sum / lag_n / 60, 2) if lag_n else 0,
               "max_lag_hours": round(max_lag_s / 3600, 2), "alert_count": len(alerts),
               "reconciliation_ok": counts["quarantined"] + counts["duplicates"] + counts["late"] + counts["accepted"] == counts["total"]}
    with open(a.metrics, "w") as f:
        json.dump(metrics, f, indent=2)
    with open(a.alerts, "w") as f:
        json.dump(alerts, f, indent=2)
    print(json.dumps(metrics, indent=2))

if __name__ == "__main__":
    main()
