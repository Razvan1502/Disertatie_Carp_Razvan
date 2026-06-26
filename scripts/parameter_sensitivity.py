from influxdb import InfluxDBClient
import pandas as pd
import os
from datetime import datetime

client = InfluxDBClient(host='localhost', port=8086, database='smartcity')

GROUPS = {
    '1': {
        'name': 'DataStreamJob — Slack Fix',
        'variants': [
            ('watermark_1s',  'Slack = 1s'),
            ('watermark_3s',  'Slack = 3s'),
            ('watermark_5s',  'Slack = 5s'),
            ('watermark_8s',  'Slack = 8s'),
            ('watermark_12s', 'Slack = 12s'),
        ],
    },
    '2': {
        'name': 'AdaptiveAdwinJob — Drift Threshold',
        'variants': [
            ('adaptive_adwin_dt500',  'driftThreshold = 500'),
            ('adaptive_adwin_dt1000', 'driftThreshold = 1000'),
            ('adaptive_adwin_dt2000', 'driftThreshold = 2000'),
        ],
    },
    '3': {
        'name': 'RealAdwinJob — L × Δδ',
        'variants': [
            ('adaptive_real_adwin_L1_dd10',    'L=0.01  Δδ=0.1'),
            ('adaptive_real_adwin_L10_dd10',   'L=0.1   Δδ=0.1'),
            ('adaptive_real_adwin_L100_dd10',  'L=1.0   Δδ=0.1'),
            ('adaptive_real_adwin_L1_dd100',   'L=0.01  Δδ=1.0'),
            ('adaptive_real_adwin_L10_dd100',  'L=0.1   Δδ=1.0'),
            ('adaptive_real_adwin_L100_dd100', 'L=1.0   Δδ=1.0'),
        ],
    },
}

print("=== Analiză Sensitivitate Parametri — Smart Home ===\n")
print("Selectează grupul de analizat:")
for k, g in GROUPS.items():
    print(f"  {k} - {g['name']}")
raw_group = input("Grup (1/2/3): ").strip()
if raw_group not in GROUPS:
    print("Grup invalid, folosesc 1.")
    raw_group = '1'
group = GROUPS[raw_group]
print(f"→ Grup: {group['name']}\n")

time_clause = "" 


THEORETICAL_MAX = 10.0
print(f"📊 Referință completeness: {THEORETICAL_MAX:.0f} evenimente/fereastră (maxim teoretic)\n")


rows = []
for sink_name, label in group['variants']:
    pts = list(client.query(f"""
        SELECT mean("true_latency_ms")       as avg_lat,
               mean("decisional_latency_ms") as avg_dec_lat,
               mean("pachete_count")         as avg_events,
               mean("update_count")          as avg_overhead
        FROM "house_metrics"
        WHERE "strategy" = '{sink_name}' AND "house_id" !~ /-initial$/ {time_clause}
    """).get_points())
    if pts:
        p = pts[0]
        rows.append({
            'Configurație':       label,
            'sink':               sink_name,
            'True Latency (s)':   round((p.get('avg_lat') or 0) / 1000, 3),
            'Decisional Lat.(s)': round((p.get('avg_dec_lat') or 0) / 1000, 3),
            'avg_events':         float(p.get('avg_events') or 0),
            'Overhead':           round(float(p.get('avg_overhead') or 0), 2),
        })
    else:
        print(f"  ⚠️  Nu am găsit date pentru '{sink_name}'")

if not rows:
    print("❌ Nicio dată găsită în InfluxDB.")
    exit()

for r in rows:
    r['Completeness (%)'] = round(
        min(100.0, r['avg_events'] / THEORETICAL_MAX * 100), 1)

df = pd.DataFrame(rows)
cols = ['Configurație', 'True Latency (s)', 'Decisional Lat.(s)', 'Completeness (%)', 'Overhead']

print("=" * 74)
print(df[cols].to_string(index=False))
print("=" * 74)

ts = datetime.now().strftime("%Y%m%d_%H%M")
out_dir = os.path.join(r'C:\Users\carpr\Disertatie\disertatie-iot\results', 'smarthome_experiments')
os.makedirs(out_dir, exist_ok=True)
csv_path = os.path.join(out_dir, f'sensitivity_grup{raw_group}_{ts}.csv')
df[cols].to_csv(csv_path, index=False)
print(f"\n✅ Salvat: {csv_path}")
print("Poți copia tabelul de mai sus direct în disertație.")
