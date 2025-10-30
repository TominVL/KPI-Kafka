# gen_var6.py
import argparse
import csv
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone, date
from typing import Optional  # <— додано

from cassandra.cluster import Cluster
from cassandra.query import PreparedStatement


BLOCK_IDS = [f"THERMAL_DN_{i}" for i in range(1, 7)] + [f"THERMAL_KH_{i}" for i in range(1, 7)]
FUEL_TYPES = ["coal", "gas", "mixed"]
STEP_SECONDS = 15  # дані кожні 15 с
# Діапазони (базові), трохи коригуються залежно від fuel_type і добового циклу:
FUEL_FLOW_BASE = (10.0, 25.0)        # т/год
STEAM_PRESSURE_BASE = (20.0, 30.0)   # МПа

KEYSPACE = "thermal_opt"


def floor_to_hour(ts: datetime) -> datetime:
    return ts.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

def to_day(ts: datetime) -> date:
    return ts.date()

def iter_times(end_ts: datetime, days: int, step_sec: int):
    """Ітеруємо всі часові мітки від (end_ts - days) до end_ts з заданим кроком."""
    start_ts = end_ts - timedelta(days=days)
    t = start_ts
    while t < end_ts:
        yield t
        t += timedelta(seconds=step_sec)

def realistic_values(fuel_type: str, t: datetime):
    """Генерація реалістичних значень з добовим циклом і шумом."""
    hour = t.hour + t.minute / 60.0
    day_cycle = 0.6 + 0.4 * (1 if 6 <= hour <= 22 else 0.6)

    ff_min, ff_max = FUEL_FLOW_BASE
    sp_min, sp_max = STEAM_PRESSURE_BASE

    if fuel_type == "coal":
        ff_shift, sp_shift = +1.5, +1.0
    elif fuel_type == "gas":
        ff_shift, sp_shift = -0.8, -0.6
    else:  # mixed
        ff_shift, sp_shift = +0.2, +0.1

    fuel_flow = random.uniform(ff_min, ff_max) * day_cycle + ff_shift + random.uniform(-0.5, 0.5)
    steam_pressure = random.uniform(sp_min, sp_max) * (0.95 + 0.1 * day_cycle) + sp_shift + random.uniform(-0.4, 0.4)

    fuel_flow = max(ff_min - 1, min(ff_max + 1, fuel_flow))
    steam_pressure = max(sp_min - 1, min(sp_max + 1, steam_pressure))
    return round(fuel_flow, 2), round(steam_pressure, 2)

# -----------------------------
# Основна логіка вставки
# -----------------------------
def insert_simple(session, days: int, limit_rows: Optional[int]):
    """
    telemetry_simple(
        block_id text, ts timestamp, fuel_type text,
        fuel_flow double, steam_pressure double,
        PRIMARY KEY (block_id, ts)
    )
    """
    ps: PreparedStatement = session.prepare(
        "INSERT INTO telemetry_simple (block_id, ts, fuel_type, fuel_flow, steam_pressure) VALUES (?,?,?,?,?)"
    )

    inserted = 0
    end_ts = datetime.now(timezone.utc)
    t0 = time.perf_counter()

    for t in iter_times(end_ts, days, STEP_SECONDS):
        for block_id in BLOCK_IDS:
            fuel_type = random.choice(FUEL_TYPES)
            fuel_flow, steam_pressure = realistic_values(fuel_type, t)
            session.execute(ps, (block_id, t, fuel_type, fuel_flow, steam_pressure))
            inserted += 1
            if limit_rows and inserted >= limit_rows:
                elapsed = time.perf_counter() - t0
                return inserted, elapsed

    elapsed = time.perf_counter() - t0
    return inserted, elapsed


def insert_hourly(session, days: int, limit_rows: Optional[int]):
    """
    telemetry_hourly(
        block_id text, fuel_type text, bucket_hour timestamp, ts timestamp,
        fuel_flow double, steam_pressure double,
        PRIMARY KEY ((block_id, fuel_type, bucket_hour), ts)
    )
    """
    ps: PreparedStatement = session.prepare(
        "INSERT INTO telemetry_hourly (block_id, fuel_type, bucket_hour, ts, fuel_flow, steam_pressure) VALUES (?,?,?,?,?,?)"
    )

    inserted = 0
    end_ts = datetime.now(timezone.utc)
    t0 = time.perf_counter()

    for t in iter_times(end_ts, days, STEP_SECONDS):
        bucket = floor_to_hour(t)
        for block_id in BLOCK_IDS:
            fuel_type = random.choice(FUEL_TYPES)
            fuel_flow, steam_pressure = realistic_values(fuel_type, t)
            session.execute(ps, (block_id, fuel_type, bucket, t, fuel_flow, steam_pressure))
            inserted += 1
            if limit_rows and inserted >= limit_rows:
                elapsed = time.perf_counter() - t0
                return inserted, elapsed

    elapsed = time.perf_counter() - t0
    return inserted, elapsed


def insert_daily(session, days: int, limit_rows: Optional[int]):
    """
    telemetry_daily_raw(
        block_id text, day date, fuel_type text, ts timestamp,
        fuel_flow double, steam_pressure double,
        PRIMARY KEY ((block_id, day, fuel_type), ts)
    )

    telemetry_daily_aggr(
        block_id text, day date, fuel_type text,
        avg_fuel_flow double, max_steam_pressure double, min_steam_pressure double,
        PRIMARY KEY (block_id, day, fuel_type)
    )
    """
    ps_raw: PreparedStatement = session.prepare(
        "INSERT INTO telemetry_daily_raw (block_id, day, fuel_type, ts, fuel_flow, steam_pressure) VALUES (?,?,?,?,?,?)"
    )
    ps_aggr: PreparedStatement = session.prepare(
        "INSERT INTO telemetry_daily_aggr (block_id, day, fuel_type, avg_fuel_flow, max_steam_pressure, min_steam_pressure) VALUES (?,?,?,?,?,?)"
    )

    aggr = {}  # key = (block_id, day, fuel_type) -> [sum_ff, cnt, max_sp, min_sp]

    inserted = 0
    end_ts = datetime.now(timezone.utc)
    t0 = time.perf_counter()

    for t in iter_times(end_ts, days, STEP_SECONDS):
        d = to_day(t)
        for block_id in BLOCK_IDS:
            fuel_type = random.choice(FUEL_TYPES)
            fuel_flow, steam_pressure = realistic_values(fuel_type, t)
            session.execute(ps_raw, (block_id, d, fuel_type, t, fuel_flow, steam_pressure))
            inserted += 1

            key = (block_id, d, fuel_type)
            if key not in aggr:
                aggr[key] = [0.0, 0, float("-inf"), float("inf")]
            A = aggr[key]
            A[0] += fuel_flow
            A[1] += 1
            A[2] = max(A[2], steam_pressure)
            A[3] = min(A[3], steam_pressure)

            if limit_rows and inserted >= limit_rows:
                elapsed = time.perf_counter() - t0
                for (bid, day_, ft), (s, c, mx, mn) in aggr.items():
                    avg_ff = round(s / c, 3) if c else 0.0
                    session.execute(ps_aggr, (bid, day_, ft, avg_ff, mx, mn))
                return inserted, elapsed

    for (bid, day_, ft), (s, c, mx, mn) in aggr.items():
        avg_ff = round(s / c, 3) if c else 0.0
        session.execute(ps_aggr, (bid, day_, ft, avg_ff, mx, mn))

    elapsed = time.perf_counter() - t0
    return inserted, elapsed


# -----------------------------
# main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Variant 6 data generator (fuel_flow, steam_pressure, fuel_type) every 15s")
    ap.add_argument("--hosts", default="127.0.0.1", help="Cassandra contact points, comma-separated")
    ap.add_argument("--keyspace", default=KEYSPACE)
    ap.add_argument("--schema", choices=["simple", "hourly", "daily"], required=True)
    ap.add_argument("--days", type=int, default=30, help="How many days to generate (default: 30)")
    ap.add_argument("--limit-rows", type=int, default=None, help="Stop after N rows (for quick benchmarks)")
    ap.add_argument("--report-csv", default=None, help="Append metrics to CSV file")
    args = ap.parse_args()

    hosts = [h.strip() for h in args.hosts.split(",") if h.strip()]

    cluster = Cluster(hosts)
    session = cluster.connect(args.keyspace)

    if args.schema == "simple":
        inserted, elapsed = insert_simple(session, args.days, args.limit_rows)
    elif args.schema == "hourly":
        inserted, elapsed = insert_hourly(session, args.days, args.limit_rows)
    else:
        inserted, elapsed = insert_daily(session, args.days, args.limit_rows)

    rps = inserted / elapsed if elapsed > 0 else 0.0
    print(f"Inserted rows: {inserted}")
    print(f"Elapsed: {elapsed:.2f}s")
    print(f"Throughput: {rps:.1f} rows/sec")

    if args.report_csv:
        new_file = not os.path.exists(args.report_csv)
        from datetime import datetime as _dt
        with open(args.report_csv, "a", newline="") as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(["schema", "days", "limit_rows", "inserted", "elapsed_sec", "rows_per_sec", "ts_utc"])
            w.writerow([args.schema, args.days, args.limit_rows or "", inserted, f"{elapsed:.2f}", f"{rps:.1f}", _dt.utcnow().isoformat()])

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
