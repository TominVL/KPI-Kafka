# Thermal Power Monitoring — Variant 6
import argparse, json, random, time, sys

STATUSES = ["normal", "startup", "shutdown", "maintenance"]
FUELS = ["coal", "gas", "mixed"]
PREFIXES = ["THERMAL_DN", "THERMAL_KH"]

rnd = random.Random()

def one_record():
    device = f"{rnd.choice(PREFIXES)}_{rnd.randint(1,6)}"
    lat = round(48.0 + rnd.random()*2.0, 4)     # 48..50
    lon = round(36.0 + rnd.random()*4.0, 4)     # 36..40
    return {
        "device_id": device,
        "power_output": round(50.0 + rnd.random()*250.0, 1),     # MW
        "efficiency":   round(35.0 + rnd.random()*7.0, 1),       # %
        "temperature":  round(400.0 + rnd.random()*200.0, 1),    # °C
        "voltage":      round(18000.0 + rnd.random()*4000.0, 1), # V
        "current":      round(3000.0 + rnd.random()*12000.0, 1), # A
        "status":       rnd.choice(STATUSES),
        "location":     {"lat": lat, "lon": lon},
        "maintenance_hours": rnd.randint(2000, 8000),
        "fuel_flow":        round(10.0 + rnd.random()*15.0, 1),  # t/h
        "steam_pressure":   round(20.0 + rnd.random()*10.0, 1),  # MPa
        "fuel_type":    rnd.choice(FUELS),
        "ts": int(time.time())
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=3, help="скільки записів згенерувати")
    ap.add_argument("--out", type=str, default="", help="шлях до файлу для збереження (JSON Lines)")
    args = ap.parse_args()

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            for _ in range(args.count):
                f.write(json.dumps(one_record(), separators=(",", ":")) + "\n")
    else:
        for _ in range(args.count):
            sys.stdout.write(json.dumps(one_record(), separators=(",", ":")) + "\n")
        sys.stdout.flush()

if __name__ == "__main__":
    main()
