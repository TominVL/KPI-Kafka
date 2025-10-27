from cassandra.cluster import Cluster
from statistics import mean

BLOCK_A = "THERMAL_DN_1"
BLOCK_B = "THERMAL_KH_2"

cluster = Cluster(["127.0.0.1"])
session = cluster.connect("thermal_ua")

def avg_power(block_id, limit=50):
    rows = session.execute(
        "SELECT power_mw FROM turbines_by_block WHERE block_id=%s LIMIT %s",
        (block_id, limit)
    )
    vals = [r.power_mw for r in rows if r.power_mw is not None]
    return mean(vals) if vals else 0.0

a = avg_power(BLOCK_A)
b = avg_power(BLOCK_B)

print(f"{BLOCK_A} – {a:.1f} МВт, {BLOCK_B} – {b:.1f} МВт "
      f"→ {'{} ефективніший'.format(BLOCK_A) if a>b else '{} ефективніший'.format(BLOCK_B) if b>a else 'рівні'}")
# Короткий висновок
if a != b:
    diff = abs(a-b)
    who = BLOCK_A if a>b else BLOCK_B
    print(f"Висновок: {who} має вищу середню потужність на {diff:.1f} МВт за останні вимірювання.")
else:
    print("Висновок: середні значення майже однакові.")
