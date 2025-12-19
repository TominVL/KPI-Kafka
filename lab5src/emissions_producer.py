import time, random
from prometheus_client import Gauge, Counter, start_http_server

# ---- Налаштування (можеш підкрутити під себе) ----
STATIONS = [f"TES_{i:02d}" for i in range(1, 11)]  # 10 ТЕС
FUELS = ["coal", "gas", "mazut"]

# Грубі емісійні фактори (умовні), "т/год" при 100% навантаженні (спрощена модель)
# (ідея: вугілля/мазут -> більше SO2, газ -> менше)
EMISSION_FACTORS = {
    "coal":  {"co2": 40.0, "so2": 0.45, "nox": 0.18},
    "gas":   {"co2": 22.0, "so2": 0.03, "nox": 0.10},
    "mazut": {"co2": 34.0, "so2": 0.60, "nox": 0.15},
}

# Ліміти (нормативи), т/год — для compliance (% від дозволеного)
LIMITS = {"co2": 30.0, "so2": 0.20, "nox": 0.12}

# "Штрафи" (умовні) — грн за (т/год) перевищення
FINE_RATE = {"co2": 500.0, "so2": 8000.0, "nox": 6000.0}

# ---- Prometheus метрики ----
power_mw = Gauge("tes_power_mw", "Потужність (МВт)", ["station", "fuel"])

emission_tph = Gauge("tes_emission_tph", "Викиди після фільтрів (т/год)", ["station", "fuel", "pollutant"])
emission_raw_tph = Gauge("tes_emission_raw_tph", "Викиди до фільтрів (т/год)", ["station", "fuel", "pollutant"])

filter_eff = Gauge("tes_filter_efficiency_percent", "Ефективність очистки (%)", ["station", "fuel", "pollutant"])
compliance_pct = Gauge("tes_compliance_percent", "Compliance (% від дозволеного)", ["station", "fuel", "pollutant"])

fine_uah_per_h = Gauge("tes_fine_uah_per_h", "Екологічний штраф (грн/год)", ["station", "fuel", "pollutant"])

violations_total = Counter("tes_violations_total", "К-сть порушень (перевищення нормативу)", ["station", "fuel", "pollutant"])
filter_fail_total = Counter("tes_filter_fail_total", "К-сть відмов системи очистки (<85%)", ["station", "fuel", "pollutant"])

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def simulate_station(station: str):
    fuel = random.choice(FUELS)

    # Навантаження: 0.35..1.00 (тобто 35–100%)
    load = random.uniform(0.35, 1.0)

    # Потужність (МВт) — просто як індикатор навантаження
    p = 300 * load + random.uniform(-10, 10)  # умовно
    p = clamp(p, 80, 320)

    # Ефективність фільтрів по кожному компоненту
    # Нормально 88–98%, інколи провал (імітація відмови)
    eff = {}
    for pol in ["co2", "so2", "nox"]:
        base = random.uniform(88, 98)

        # шанс “відмови” 2% -> різко падає
        if random.random() < 0.02:
            base = random.uniform(60, 84)
        eff[pol] = clamp(base, 0, 100)

    # Викиди: залежать від палива та навантаження (вимога) :contentReference[oaicite:1]{index=1}
    for pol in ["co2", "so2", "nox"]:
        raw = EMISSION_FACTORS[fuel][pol] * load * random.uniform(0.90, 1.10)
        after = raw * (1.0 - eff[pol] / 100.0)

        # compliance (% від дозволеного) (вимога) :contentReference[oaicite:2]{index=2}
        comp = (after / LIMITS[pol]) * 100.0

        # штраф: тільки за перевищення
        exceed = max(0.0, after - LIMITS[pol])
        fine = exceed * FINE_RATE[pol]

        # метрики
        power_mw.labels(station=station, fuel=fuel).set(p)
        emission_raw_tph.labels(station=station, fuel=fuel, pollutant=pol).set(raw)
        emission_tph.labels(station=station, fuel=fuel, pollutant=pol).set(after)
        filter_eff.labels(station=station, fuel=fuel, pollutant=pol).set(eff[pol])
        compliance_pct.labels(station=station, fuel=fuel, pollutant=pol).set(comp)
        fine_uah_per_h.labels(station=station, fuel=fuel, pollutant=pol).set(fine)

        if after > LIMITS[pol]:
            violations_total.labels(station=station, fuel=fuel, pollutant=pol).inc()

        if eff[pol] < 85:
            filter_fail_total.labels(station=station, fuel=fuel, pollutant=pol).inc()

def main():
    start_http_server(8000)
    print("Metrics: http://localhost:8000/metrics")

    while True:
        for st in STATIONS:
            simulate_station(st)
        time.sleep(15)  # дані кожні 15 секунд (контекст варіанту) :contentReference[oaicite:3]{index=3}

if __name__ == "__main__":
    main()
