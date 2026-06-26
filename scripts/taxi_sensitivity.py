from influxdb import InfluxDBClient
import pandas as pd
import os
from datetime import datetime

client = InfluxDBClient(host='localhost', port=8086, database='smartcity')

GROUPS = {
    '1': {
        'name': 'TaxiDataStreamJob — Slack Fix',
        'variants': [
            ('taxi_watermark_m3_s500',  'Slack = 500s'),
            ('taxi_watermark_m3_s900',  'Slack = 900s'),
            ('taxi_watermark_m3_s1500', 'Slack = 1500s (default)'),
            ('taxi_watermark_m3_s2400', 'Slack = 2400s'),
            ('taxi_watermark_m3_s3600', 'Slack = 3600s'),
        ],
    },
    '2': {
        'name': 'TaxiAdaptiveAdwinJob — Drift Threshold',
        'variants': [
            ('taxi_adaptive_adwin_m3_dt30',  'driftThreshold = 30s'),
            ('taxi_adaptive_adwin_m3_dt60',  'driftThreshold = 60s (default)'),
            ('taxi_adaptive_adwin_m3_dt120', 'driftThreshold = 120s'),
        ],
    },
    '3': {
        'name': 'TaxiRealAdwinJob — L × Δδ',
        'variants': [
            ('taxi_real_adwin_m3_L1_dd10',    'L=0.01  Δδ=0.1'),
            ('taxi_real_adwin_m3_L10_dd10',   'L=0.1   Δδ=0.1 (default)'),
            ('taxi_real_adwin_m3_L100_dd10',  'L=1.0   Δδ=0.1'),
            ('taxi_real_adwin_m3_L1_dd100',   'L=0.01  Δδ=1.0'),
            ('taxi_real_adwin_m3_L10_dd100',  'L=0.1   Δδ=1.0'),
            ('taxi_real_adwin_m3_L100_dd100', 'L=1.0   Δδ=1.0'),
        ],
    },
}

print("=== Analiză Sensitivitate Parametri — Taxi NYC ===\n")
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

def get_spec_reference(time_clause):
    """Medie a count-ului FINAL per (zonă, fereastră) pentru Speculative.
    Speculative emite de N ori per fereastră (CountTrigger), cu counts 1,2,...,N.
    mean(toate) ar subestima — luăm max per (zonă, window_end), identic cu taxi_comparison.py.
    """
    result = client.query(f"""
        SELECT "window_end", "event_count"
        FROM "taxi_metrics"
        WHERE "strategy" = 'taxi_speculative_m3' {time_clause}
        GROUP BY "zone_id"
    """)
    all_finals = []
    for (_, tags), points in result.items():
        df_zone = pd.DataFrame(list(points))
        if df_zone.empty or 'event_count' not in df_zone.columns:
            continue
        
        zone_finals = df_zone.groupby('window_end')['event_count'].max()
        all_finals.extend(zone_finals.tolist())
    return float(pd.Series(all_finals).mean()) if all_finals else 0.0

print("⏳ Calculez referința Speculative (max per fereastră)...")
spec_events = get_spec_reference(time_clause)

if spec_events > 0:
    print(f"📊 Referință: Speculative ({spec_events:.1f} curse/fereastră — count final)\n")
else:
    print("ℹ️  Speculative absent — completeness față de varianta cu cele mai multe curse.\n")


rows = []
for sink_name, label in group['variants']:
    pts = list(client.query(f"""
        SELECT mean("true_latency_ms") as avg_lat,
               mean("event_count")     as avg_events,
               mean("window_delay_s")  as avg_delay_s
        FROM "taxi_metrics"
        WHERE "strategy" = '{sink_name}' {time_clause}
    """).get_points())
    if pts:
        p = pts[0]
        rows.append({
            'Configurație':        label,
            'sink':                sink_name,
            'True Latency (s)':    round((p.get('avg_lat') or 0) / 1000, 3),
            'Window Delay (min)':  round((p.get('avg_delay_s') or 0) / 60, 2),
            'avg_events':          float(p.get('avg_events') or 0),
        })
    else:
        print(f"  ⚠️  Nu am găsit date pentru '{sink_name}'")

if not rows:
    print("❌ Nicio dată găsită în InfluxDB.")
    exit()

ref_events = spec_events if spec_events > 0 else max(r['avg_events'] for r in rows)
ref_label  = "Speculative (count final)" if spec_events > 0 else "max dintre variante"
print(f"📊 Referință completeness: {ref_events:.1f} curse/fereastră ({ref_label})\n")

for r in rows:
    r['Completeness (%)'] = round(
        min(100.0, r['avg_events'] / ref_events * 100) if ref_events > 0 else 0.0, 1)

df = pd.DataFrame(rows)
cols = ['Configurație', 'True Latency (s)', 'Window Delay (min)', 'Completeness (%)']

print("=" * 70)
print(df[cols].to_string(index=False))
print("=" * 70)

ts = datetime.now().strftime("%Y%m%d_%H%M")
out_dir = os.path.join(r'C:\Users\carpr\Disertatie\disertatie-iot\results', 'taxi_experiments')
os.makedirs(out_dir, exist_ok=True)
csv_path = os.path.join(out_dir, f'taxi_sensitivity_grup{raw_group}_{ts}.csv')
df[cols].to_csv(csv_path, index=False)
print(f"\n✅ Salvat: {csv_path}")
print("Poți copia tabelul de mai sus direct în disertație.")
