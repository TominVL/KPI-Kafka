# stream_windows.py
import json, time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from kafka import KafkaConsumer
from cassandra.cluster import Cluster

WIN_MIN  = 30          # тривалість вікна
HOP_MIN  = 10          # крок
GRACE_S  = 60          # затримка для «пізніх» подій

def to_dt_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms/1000, tz=timezone.utc)

def floor_to_10min(dt: datetime) -> datetime:
    return dt.replace(minute=(dt.minute//10)*10, second=0, microsecond=0)

def windows_for(ts: datetime):
    base = floor_to_10min(ts)
    return [base - timedelta(minutes=10*i) for i in range(0, WIN_MIN//HOP_MIN)]

def heat_rate(avg_flow: float, avg_press: float) -> float:
    # спрощена метрика, щоб було видно зміну; шкалуємо трохи
    return round((avg_flow / max(avg_press, 1e-6)) * 1.2, 6)

cluster = Cluster(["127.0.0.1"])
sess = cluster.connect("thermal_streams")
ins = sess.prepare("""
INSERT INTO fuel_optimization_aggregates
(station_id, block_id, fuel_type, window_start, window_end,
 avg_fuel_flow, avg_steam_pressure, heat_rate, records)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# ключ стану: (station_id, block_id, fuel_type, window_start)
State = defaultdict(lambda: {"sum_flow":0.0, "sum_press":0.0, "n":0, "last":None})
state: dict = State()

def add_event(ev: dict):
    # очікуємо 'ts' у мс
    ts_dt = to_dt_ms(int(ev["ts"]))
    for ws in windows_for(ts_dt):
        key = (ev["station_id"], ev["block_id"], ev["fuel_type"], ws)
        st = state[key]
        st["sum_flow"]  += float(ev["fuel_flow"])
        st["sum_press"] += float(ev["steam_pressure"])
        st["n"] += 1
        st["last"] = ts_dt

def flush_expired(now_dt: datetime):
    to_delete = []
    for (station, block, fuel, ws), st in list(state.items()):
        we = ws + timedelta(minutes=WIN_MIN)
        if now_dt > we + timedelta(seconds=GRACE_S):
            avg_flow  = st["sum_flow"]/st["n"]
            avg_press = st["sum_press"]/st["n"]
            sess.execute(ins, (
                station, block, fuel, ws, we,
                avg_flow, avg_press, heat_rate(avg_flow, avg_press), st["n"]
            ))
            to_delete.append((station, block, fuel, ws))
    for k in to_delete:
        state.pop(k, None)

if __name__ == "__main__":
    cons = KafkaConsumer(
        "thermal.telemetry",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="win-agg-1",
    )
    print("Streaming aggregator started… (30m windows / 10m hop)")
    last = time.time()
    try:
        for msg in cons:
            ev = msg.value
            # обов’язкові поля
            if not all(k in ev for k in ("station_id","block_id","fuel_type","ts","fuel_flow","steam_pressure")):
                continue
            add_event(ev)
            if time.time() - last > 10:
                flush_expired(datetime.now(tz=timezone.utc))
                last = time.time()
    except KeyboardInterrupt:
        print("Stopped.")
