#!/usr/bin/env bash
set -euo pipefail

# === ПАРАМЕТРИ (можеш змінити) ==========================
KS=${KS:-thermal_opt}
BLOCK=${BLOCK:-THERMAL_DN_1}
FUEL=${FUEL:-coal}
DAY=${DAY:-2025-10-26}          # доба для daily_raw та агрегацій
H16=${H16:-2025-10-26T16:00:00Z}
H20=${H20:-2025-10-26T20:00:00Z}
H21=${H21:-2025-10-26T21:00:00Z}
H22=${H22:-2025-10-26T22:00:00Z}
RUNS=${RUNS:-10}
OUT=${OUT:-latency_table1.csv}

# P95/P99 із твоїх tablehistograms (у мс)
P95_SIMPLE=0.924; P99_SIMPLE=0.924
P95_HOURLY=0.770; P99_HOURLY=0.770
P95_DAILY=0.446;  P99_DAILY=0.446

avg_ms () {
  local q="$1"
  local runs="${2:-$RUNS}"
  local sum=0
  for _ in $(seq 1 "$runs"); do
    us=$(cqlsh -e "TRACING ON; $q" 2>/dev/null | \
         awk -F'|' '/Request complete/ {gsub(/ /,"",$4); print $4; exit}')
    : "${us:=0}"
    sum=$((sum + us))
  done
  # повертаємо середнє в мілісекундах з 2 знаками
  awk -v s="$sum" -v n="$runs" 'BEGIN {printf "%.2f", (s/n)/1000.0}'
}

# Заголовок CSV
echo "Schema,Query Type,Avg Latency (ms),P95 (ms),P99 (ms)" > "$OUT"

# -------- SIMPLE ----------
AVG=$(avg_ms "SELECT ts,fuel_flow,steam_pressure FROM $KS.telemetry_simple WHERE block_id='$BLOCK' AND ts>='$H20' AND ts<'$H21' LIMIT 500;")
echo "Simple,Latest hour,$AVG,$P95_SIMPLE,$P99_SIMPLE" >> "$OUT"

AVG=$(avg_ms "SELECT ts,fuel_flow,steam_pressure FROM $KS.telemetry_simple WHERE block_id='$BLOCK' AND ts>='$H16' AND ts<'$H22' LIMIT 500;")
echo "Simple,6 hour range,$AVG,$P95_SIMPLE,$P99_SIMPLE" >> "$OUT"

AVG=$(avg_ms "SELECT * FROM $KS.telemetry_daily_aggr WHERE block_id='$BLOCK' AND day='$DAY' AND fuel_type='$FUEL';")
echo "Simple,Daily aggregation,$AVG,$P95_SIMPLE,$P99_SIMPLE" >> "$OUT"

# -------- HOURLY ----------
AVG=$(avg_ms "SELECT ts,fuel_flow,steam_pressure FROM $KS.telemetry_hourly WHERE block_id='$BLOCK' AND fuel_type='$FUEL' AND bucket_hour='$H21' AND ts>='$H20' AND ts<'$H22' LIMIT 500;")
echo "Hourly,Latest hour,$AVG,$P95_HOURLY,$P99_HOURLY" >> "$OUT"

AVG=$(avg_ms "SELECT ts,fuel_flow,steam_pressure FROM $KS.telemetry_hourly WHERE block_id='$BLOCK' AND fuel_type='$FUEL' AND bucket_hour IN ('$H16','2025-10-26T17:00:00Z','2025-10-26T18:00:00Z','2025-10-26T19:00:00Z','$H20','$H21') AND ts>='$H16' AND ts<'$H22' LIMIT 500;")
echo "Hourly,6 hour range,$AVG,$P95_HOURLY,$P99_HOURLY" >> "$OUT"

AVG=$(avg_ms "SELECT * FROM $KS.telemetry_daily_aggr WHERE block_id='$BLOCK' AND day='$DAY' AND fuel_type='$FUEL';")
echo "Hourly,Daily aggregation,$AVG,$P95_HOURLY,$P99_HOURLY" >> "$OUT"

# -------- DAILY (raw) ------
AVG=$(avg_ms "SELECT ts,fuel_flow,steam_pressure FROM $KS.telemetry_daily_raw WHERE block_id='$BLOCK' AND day='$DAY' AND fuel_type='$FUEL' AND ts>='$H21' AND ts<'$H22' LIMIT 500;")
echo "Daily,Latest hour,$AVG,$P95_DAILY,$P99_DAILY" >> "$OUT"

AVG=$(avg_ms "SELECT ts,fuel_flow,steam_pressure FROM $KS.telemetry_daily_raw WHERE block_id='$BLOCK' AND day='$DAY' AND fuel_type='$FUEL' AND ts>='$H16' AND ts<'$H22' LIMIT 500;")
echo "Daily,6 hour range,$AVG,$P95_DAILY,$P99_DAILY" >> "$OUT"

AVG=$(avg_ms "SELECT * FROM $KS.telemetry_daily_aggr WHERE block_id='$BLOCK' AND day='$DAY' AND fuel_type='$FUEL';")
echo "Daily,Daily aggregation,$AVG,$P95_DAILY,$P99_DAILY" >> "$OUT"

echo "Saved -> $OUT"
