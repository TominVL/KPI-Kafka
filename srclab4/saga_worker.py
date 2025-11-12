# saga_worker.py
import json, uuid
from datetime import datetime, timezone
from kafka import KafkaConsumer
from cassandra.cluster import Cluster

def now(): return datetime.now(tz=timezone.utc)

cluster = Cluster(["127.0.0.1"])
sess = cluster.connect("thermal_streams")

log_ins = sess.prepare("""
INSERT INTO saga_log (saga_id, step, status, station_id, fuel_type, amount_t, event_time, payload)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")

inv_sel_latest = sess.prepare("""
SELECT available_t, reserved_t FROM fuel_inventory
WHERE station_id=? AND fuel_type=? LIMIT 1
""")

inv_ins = sess.prepare("""
INSERT INTO fuel_inventory (station_id, fuel_type, ts, available_t, reserved_t)
VALUES (?, ?, ?, ?, ?)
""")

def slog(saga_id, step, status, st, fuel, amt, payload):
    sess.execute(log_ins, (saga_id, step, status, st, fuel, amt, now(), json.dumps(payload)))

def handle_event(raw_bytes: bytes):
    data = json.loads(raw_bytes.decode("utf-8"))
    saga_id = uuid.UUID(data["saga_id"]) if "saga_id" in data else uuid.uuid4()
    kind    = data["type"]                # 'reserve' | 'commit' | 'cancel'
    st      = data["station_id"]
    fuel    = data["fuel_type"]
    amt     = float(data.get("amount_t", 0.0))

    row    = sess.execute(inv_sel_latest, (st, fuel)).one()
    avail  = float(getattr(row, "available_t", 1000.0) or 1000.0)  # дефолтний стартовий запас
    reserv = float(getattr(row, "reserved_t",  0.0) or 0.0)

    if kind == "reserve":
        if avail - reserv >= amt:
            sess.execute(inv_ins, (st, fuel, now(), avail, reserv + amt))
            slog(saga_id, 1, "RESERVED", st, fuel, amt, data)
        else:
            slog(saga_id, 1, "REJECTED", st, fuel, amt, data)

    elif kind == "commit":
        if reserv >= amt:
            sess.execute(inv_ins, (st, fuel, now(), avail - amt, reserv - amt))
            slog(saga_id, 2, "COMMITTED", st, fuel, amt, data)
        else:
            slog(saga_id, 2, "FAILED", st, fuel, amt, data)

    elif kind == "cancel":
        if reserv >= amt:
            sess.execute(inv_ins, (st, fuel, now(), avail, reserv - amt))
            slog(saga_id, -1, "COMPENSATED", st, fuel, amt, data)
        else:
            slog(saga_id, -1, "FAILED", st, fuel, amt, data)

if __name__ == "__main__":
    cons = KafkaConsumer(
        "procurement",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="saga-1",
    )
    print("Saga worker started… (fuel_inventory with ts, available_t/reserved_t)")
    try:
        for msg in cons:
            handle_event(msg.value)
    except KeyboardInterrupt:
        pass
