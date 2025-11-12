# telemetry_gen.py
# Генератор телеметрії (~2.1 msg/s) у Kafka topic thermal.telemetry
import argparse, json, random, time
from datetime import datetime, timezone
from kafka import KafkaProducer

STATIONS = [f"ST{i}" for i in range(1, 9)]  # 8 ТЕС
REGIONS  = ["DN","KH","ZP","SU","LV","OD","KY","CK"]
BLOCKS   = [f"THERMAL_{r}_{b}" for r in REGIONS for b in range(1, 5)]  # 32 блоки
FUEL     = ["coal","gas","mixed"]

def now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp()*1000)

def mk_event() -> dict:
    station = random.choice(STATIONS)
    block   = random.choice(BLOCKS)
    fuel    = random.choices(FUEL, weights=[0.5,0.3,0.2])[0]
    return {
        "station_id": station,
        "block_id": block,
        "ts": now_ms(),                         # мс (UTC)
        "fuel_flow": round(random.uniform(10.0, 25.0), 2),
        "steam_pressure": round(random.uniform(20.0, 30.0), 2),
        "steam_temperature": round(random.uniform(480, 560), 1),
        "flue_gas_temp": round(random.uniform(120, 180), 1),
        "NOx_emissions": round(random.uniform(150, 280), 1),
        "SOx_emissions": round(random.uniform(120, 240), 1),
        "CO2_emissions": round(random.uniform(0.8, 1.3), 3),
        "fuel_type": fuel
    }

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="thermal.telemetry")
    ap.add_argument("--rate", type=float, default=2.1)
    args = ap.parse_args()

    prod = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    period = max(0.001, 1.0/args.rate)
    print(f"Producing to {args.topic} at ~{args.rate} msg/s. Ctrl+C to stop.")
    try:
        while True:
            prod.send(args.topic, mk_event())
            prod.flush()
            time.sleep(period)
    except KeyboardInterrupt:
        pass
