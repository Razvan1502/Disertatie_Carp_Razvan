from influxdb import InfluxDBClient
import csv
import os
import time
from datetime import datetime, timezone

client = InfluxDBClient(host='localhost', port=8086, database='smartcity')

INTERVAL_MIN = 10

raw_mode = input("Modul de rulare (1, 2, sau 3 — implicit 3): ").strip()
mode = int(raw_mode) if raw_mode in ('1', '2', '3') else 3

STRATEGIES = [
    'taxi_naive',
    f'taxi_watermark_m{mode}',
    f'taxi_adaptive_adwin_m{mode}',
    f'taxi_real_adwin_m{mode}',
    f'taxi_speculative_m{mode}',
]
LABELS = {
    'taxi_naive':                       'Naiv (fără OOO)',
    f'taxi_watermark_m{mode}':          f'Watermark Fix ({"1500s" if mode == 3 else "36s"})',
    f'taxi_adaptive_adwin_m{mode}':     'Heuristic Adaptive Watermark',
    f'taxi_real_adwin_m{mode}':         'Official ADWIN (Awad)',
    f'taxi_speculative_m{mode}':        'Speculative [REF]',
}

LOG_PATH = os.path.join(
    r'C:\Users\carpr\Disertatie\disertatie-iot\results',
    f'monitor_m{mode}_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
)


def query_interval(strategy, minutes):
    q = f"""
    SELECT mean("event_count")    as avg_events,
           mean("true_latency_ms") as avg_latency,
           count("event_count")   as windows_fired
    FROM "taxi_metrics"
    WHERE "strategy" = '{strategy}' AND time > now() - {minutes}m
    """
    res = client.query(q)
    for (_, _), pts in res.items():
        for p in pts:
            return {
                'avg_events':    float(p.get('avg_events')  or 0),
                'avg_latency_ms': float(p.get('avg_latency') or 0),
                'windows_fired': int(p.get('windows_fired') or 0),
            }
    return {'avg_events': 0, 'avg_latency_ms': 0, 'windows_fired': 0}


def print_table(rows, spec_avg, ts):
    w = 76
    print(f"\n{'═' * w}")
    print(f"  ⏱  {ts}  |  Interval analizat: ultimele {INTERVAL_MIN} minute  |  Mod: {mode}")
    print(f"  Speculative (ref): {spec_avg:.1f} curse/fereastră în medie")
    print(f"{'═' * w}")
    print(f"  {'Strategie':<26} {'Completeness':>12}  {'Curse pierdute':>15}  {'Latency medie':>13}")
    print(f"  {'-'*26} {'-'*12}  {'-'*15}  {'-'*13}")
    for r in rows:
        is_ref = r['strategy'] == f'taxi_speculative_m{mode}'
        comp   = 100.0 if is_ref else (min(100.0, r['avg_events'] / spec_avg * 100) if spec_avg > 0 else 0.0)
        lost   = 0.0   if is_ref else max(0.0, spec_avg - r['avg_events'])
        lat    = r['avg_latency_ms'] / 1000.0
        lost_str = '—' if is_ref else f"{lost:.1f}"
        print(f"  {r['label']:<26} {comp:>11.1f}%  {lost_str:>15}  {lat:>12.2f}s")
    print(f"{'═' * w}")


def append_csv(rows, spec_avg, ts):
    new_file = not os.path.exists(LOG_PATH)
    with open(LOG_PATH, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(['timestamp', 'mode', 'strategy', 'completeness_pct',
                             'avg_events', 'lost_events_per_window',
                             'avg_latency_s', 'windows_fired'])
        for r in rows:
            is_ref = r['strategy'] == f'taxi_speculative_m{mode}'
            comp   = 100.0 if is_ref else (min(100.0, r['avg_events'] / spec_avg * 100) if spec_avg > 0 else 0.0)
            lost   = 0.0   if is_ref else max(0.0, spec_avg - r['avg_events'])
            writer.writerow([
                ts,
                mode,
                r['strategy'],
                round(comp, 2),
                round(r['avg_events'], 2),
                round(lost, 2),
                round(r['avg_latency_ms'] / 1000, 3),
                r['windows_fired'],
            ])


print("╔══════════════════════════════════════════╗")
print("║    Monitor Live — Pipeline Taxi NYC      ║")
print("╚══════════════════════════════════════════╝")
print(f"  Mod: {mode}  |  Interval: {INTERVAL_MIN} minute  |  Log: {LOG_PATH}")
print("  Apasă Ctrl+C pentru a opri.\n")

spec_key = f'taxi_speculative_m{mode}'
iteration = 0
while True:
    iteration += 1
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    spec = query_interval(spec_key, INTERVAL_MIN)
    spec_avg = spec['avg_events']

    if spec_avg == 0:
        print(f"[{ts}] ⏳ Aștept date de la Speculative... (job pornit?)")
    else:
        rows = []
        for s in STRATEGIES:
            d = query_interval(s, INTERVAL_MIN)
            rows.append({'strategy': s, 'label': LABELS[s], **d})

        print_table(rows, spec_avg, ts)
        append_csv(rows, spec_avg, ts)
        print(f"  ✅ Iterația #{iteration} salvată în CSV.")

    print(f"  ⏳ Următoarea actualizare în {INTERVAL_MIN} minute...\n")
    time.sleep(INTERVAL_MIN * 60)
