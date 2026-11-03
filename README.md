# Streaming Data Pipeline — SYNTHETIC DATA

**Business question:** An order-event stream arrives out of order, with
duplicate deliveries and malformed rows. How do you turn it into
trustworthy hourly revenue numbers *while the stream is running* —
without reprocessing the whole dataset every time?

**Why this project:** The latest Data Engineer postings (early Nov 2026)
keep asking for exactly this: ingestion pipelines plus pipeline
monitoring/observability (DRW), a data-execution layer (Mixpanel),
streaming ingestion on Kafka/Kinesis/Pub/Sub (FieldAI, InductiveHealth),
and SQL window/aggregation skill (Splice). Project 11 in this portfolio
covered the same event domain as a *batch* bronze/silver/gold pipeline;
this project is the streaming counterpart — incremental aggregation,
watermarks, and alerts as events arrive. It is a portfolio skill demo
built from synthetic data. It is **not** work experience and the numbers
below are **not** real company results.

**Honest labelling of the stack:** there is no Kafka broker in this
repo. `data/raw_stream.jsonl` is a Kafka-style log — events in received
order, with duplicates and late arrivals — and `src/stream_pipeline.py`
is the consumer: it reads the log once, in order, and maintains its
aggregates incrementally. The concepts transfer directly (event log,
consumer offsets in order, watermark, exactly-once via dedup by
event_id); nothing here should be read as production Kafka operations
experience.

## How it works

```
raw_stream.jsonl (received order: duplicates, late, malformed)
      │  consumer, one pass, in order
      ├─► quarantine_events   (malformed + reason, auditable)
      ├─► duplicates dropped  (same event_id seen before)
      ├─► late_events         (window already closed by watermark)
      ▼
window_aggregates (SQLite + CSV, 1-hour tumbling windows)
      │
      └─► results/alerts.json (monitoring: duplicate rate, lag, lateness)
```

- **Validation first:** null amount, negative purchase amount, and
  unknown event types are quarantined with a reason, never silently
  dropped.
- **Dedup by event_id:** a re-delivered event counts once.
- **Watermark:** `max event_ts seen − 60 minutes`. A window whose end
  is at or before the watermark is closed and frozen; an event arriving
  for a closed window is recorded as late and is *not* folded into the
  frozen aggregate, so published numbers never change retroactively.
- **Incremental aggregation:** each accepted event updates only its own
  hour's counters — no full re-scan.
- **Monitoring:** alerts fire on duplicate rate > 2%, max consumer lag
  > 6 hours, and late-event rate > 1%.

## Reproduce

```bash
python3 src/generate_stream.py --out data/raw_stream.jsonl   # seed 20261103
python3 src/stream_pipeline.py                                # writes results/
```

Stdlib only — no packages to install. A from-scratch re-run (outputs
deleted, pipeline re-run) reproduces `results/metrics.json` exactly.

## Real results (from the actual run, seed 20261103)

- Input: **5,210** events (5,000 generated + 150 duplicate deliveries +
  60 malformed rows), Jun 1–3, 2026, synthetic.
- Reconciliation (holds exactly): 60 quarantined + 150 duplicates +
  214 late + 4,786 accepted = 5,210.
- Output: **72 hourly windows** — 70 closed by watermark, 2 flushed at
  end of stream. Total net revenue across accepted events:
  **$1,026,877.13**.
- Duplicate rate **2.88%**, late-event rate **4.11%**, average consumer
  lag **60.19 minutes**, max lag **29.89 hours** (the deliberately late
  arrivals).
- Alerts fired (3): high duplicate rate, high consumer lag,
  late events present. No hourly window breached the 20% refund-rate
  threshold, so no refund alert fired — reported as-is, not tuned to
  force one.
- Cross-check: the SQLite `window_aggregates` table and
  `results/window_aggregates.csv` agree (72 rows, same net-revenue
  total); quarantine (60) and late (214) tables match the metrics.

## Files

- `src/generate_stream.py` — seeded synthetic stream generator
- `src/stream_pipeline.py` — streaming consumer (validate, dedup,
  watermark, window, alert)
- `data/raw_stream.jsonl` — the event log, in received order
- `results/window_aggregates.csv`, `results/metrics.json`,
  `results/alerts.json` — outputs of the run above
