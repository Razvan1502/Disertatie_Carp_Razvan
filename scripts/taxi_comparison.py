from influxdb import InfluxDBClient
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime

run_name = input("Nume rulare (ex: taxi_mode3): ").strip()
if not run_name:
    run_name = "taxi_run"
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
folder_name = f"{run_name}_{timestamp}"

RESULTS = os.path.join(r'C:\Users\carpr\Disertatie\disertatie-iot\results', folder_name)
os.makedirs(RESULTS, exist_ok=True)
print(f"📁 Rezultate salvate în: {RESULTS}\n")

time_window = "" 
print("→ Fereastra: toate datele din DB")

raw_mode = input("Modul de rulare (1, 2, sau 3 — implicit 3): ").strip()
mode = int(raw_mode) if raw_mode in ('1', '2', '3') else 3
print(f"→ Modul selectat: {mode}")

raw_bucket = input("Bucket timeseries (ex: 2m, 5m, 10m — implicit 2m pt faze scurte, 10m pt faze lungi): ").strip()
ts_bucket = raw_bucket if raw_bucket else ("10m" if mode == 3 else "2m")
print(f"→ Bucket timeseries: {ts_bucket}")


S_NAIVE       = 'taxi_naive'
S_WATERMARK   = f'taxi_watermark_m{mode}_s1500'    if mode == 3 else f'taxi_watermark_m{mode}'
S_ADAPTIVE    = f'taxi_adaptive_adwin_m{mode}_dt60' if mode == 3 else f'taxi_adaptive_adwin_m{mode}'
S_REAL_ADWIN  = f'taxi_real_adwin_m{mode}_L10_dd10' if mode == 3 else f'taxi_real_adwin_m{mode}'
S_SPECULATIVE = f'taxi_speculative_m{mode}'
ALL_STRATEGIES = [S_NAIVE, S_WATERMARK, S_ADAPTIVE, S_REAL_ADWIN, S_SPECULATIVE]

print("🔍 Conectare la InfluxDB...")
client = InfluxDBClient(host='localhost', port=8086, database='smartcity')

LABELS = {
    S_NAIVE:       'Naiv\n(fără OOO)',
    S_WATERMARK:   f'Watermark Fix\n({"1500s" if mode == 3 else "36s"})',
    S_ADAPTIVE:    'Heuristic Adaptive\nWatermark',
    S_REAL_ADWIN:  'Official ADWIN\n(Awad 2019)',
    S_SPECULATIVE: 'Speculative',
}
COLORS = {
    S_NAIVE:       '#7f8c8d',
    S_WATERMARK:   '#e74c3c',
    S_REAL_ADWIN:  '#f39c12',
    S_SPECULATIVE: '#2ecc71',
    S_ADAPTIVE:    '#3498db',
}

sns.set_theme(style="whitegrid", context="paper", font_scale=1.2)

# Helper functions 

def query_per_window_data(influx_client, strategy, tw="2h"):
    """Query per-window records grouped by zone_id (tag), filter out CELL_OUT."""
    where_time = f"AND time > now() - {tw}" if tw else ""
    q = f"""
    SELECT "window_end", "avg_distance", "avg_duration", "event_count", "update_count"
    FROM "taxi_metrics"
    WHERE "strategy" = '{strategy}' {where_time}
    GROUP BY "zone_id"
    """
    res = influx_client.query(q)
    rows = []
    for (_, tags), pts in res.items():
        zone_id = tags.get('zone_id', '') if tags else ''
        if zone_id == 'CELL_OUT':
            continue
        for p in pts:
            if p.get('window_end') is not None:
                rows.append({
                    'window_end':   int(p['window_end']),
                    'zone_id':      zone_id,
                    'avg_distance': float(p.get('avg_distance') or 0),
                    'avg_duration': float(p.get('avg_duration') or 0),
                    'event_count':  int(p.get('event_count') or 0),
                    'update_count': int(p.get('update_count') or 1),
                })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_speculative_final_stats(influx_client, strategy, tw):
    """Return final avg_distance and avg_duration from Speculative's last update per window.
    Uses max event_count per (window_end, zone_id) to avoid bias from intermediate updates.
    """
    raw = query_per_window_data(influx_client, strategy, tw)
    if raw.empty:
        return 0.0, 0.0
    final = (raw.sort_values('event_count')
               .drop_duplicates(['window_end', 'zone_id'], keep='last'))
    return final['avg_distance'].mean(), final['avg_duration'].mean()


def get_speculative_true_baseline(influx_client, strategy, tw):
    """Return average FINAL event_count per (window_end, zone_id) for Speculative.
    Takes max event_count per window (= the last/most-complete update), then averages.
    This avoids the distortion from intermediate speculative updates.
    """
    raw = query_per_window_data(influx_client, strategy, tw)
    if raw.empty:
        return 0.0
    final = (raw.sort_values('event_count')
               .drop_duplicates(['window_end', 'zone_id'], keep='last'))
    return final['event_count'].mean()


def query_zone_events_speculative(influx_client, strategy, tw):
    """Like query_zone_events but uses final (max) event_count per window per zone.
    Needed for Speculative which emits many intermediate updates.
    """
    raw = query_per_window_data(influx_client, strategy, tw)
    if raw.empty:
        return {}
    final = (raw.sort_values('event_count')
               .drop_duplicates(['window_end', 'zone_id'], keep='last'))
    return final.groupby('zone_id')['event_count'].mean().to_dict()


# SECȚIUNEA 1: Statistici globale per strategie (graficele 1–5)

time_clause = f"AND time > now() - {time_window}" if time_window else ""
strat_filter = " OR ".join([f'"strategy" = \'{s}\'' for s in ALL_STRATEGIES])

query = f"""
SELECT
    mean("true_latency_ms")    as avg_true_latency,
    mean("window_delay_s") as avg_window_delay,
    mean("event_count")        as avg_events,
    mean("avg_distance")       as avg_distance,
    mean("avg_duration")       as avg_duration,
    mean("update_count")       as avg_overhead
FROM "taxi_metrics"
WHERE ({strat_filter}) {time_clause}
AND "window_delay_s" < 86400
GROUP BY "strategy"
"""

result = client.query(query)

data = []
for (measurement, tags), points in result.items():
    strategy = tags['strategy']
    for point in points:
        data.append({
            'Strategy':               strategy,
            'True Latency (s)':       (point['avg_true_latency'] or 0) / 1000.0,
            'Window Delay (min)': (point['avg_window_delay'] or 0) / 60.0,
            'Avg Events/Window':      point['avg_events'] or 0,
            'Avg Distance (mi)':      point['avg_distance'] or 0,
            'Avg Duration (min)':     point['avg_duration'] or 0,
            'Overhead (Updates)':     point['avg_overhead'] or 0,
        })

if not data:
    print("❌ Nu am găsit date în taxi_metrics! Lasă job-urile să ruleze câteva minute.")
    exit()

df = pd.DataFrame(data)

df_main = df.copy()

spec_events_true = get_speculative_true_baseline(client, S_SPECULATIVE, time_window)
if spec_events_true > 0:
    df_main['Completeness (%)'] = (df_main['Avg Events/Window'] / spec_events_true * 100).clip(upper=100)
    df_main.loc[df_main['Strategy'] == S_SPECULATIVE, 'Completeness (%)'] = 100.0
else:
    df_main['Completeness (%)'] = df_main['Avg Events/Window']

df_main['Label'] = df_main['Strategy'].map(lambda s: LABELS.get(s, s))
palette_main = [COLORS.get(s, '#95a5a6') for s in df_main['Strategy']]

print("\n✅ Date extrase:")
print(df_main[['Strategy', 'True Latency (s)', 'Window Delay (min)',
               'Avg Events/Window', 'Completeness (%)', 'Overhead (Updates)']].to_string(index=False))

# Graf 1: Window Delay (latența reală în deployment real)
plt.figure(figsize=(9, 6))
ax = sns.barplot(x='Label', y='Window Delay (min)', hue='Label', data=df_main,
                 palette=palette_main, legend=False)
plt.title('Window Delay — Dataset Taxi NYC\n'
          '(watermark_la_trigger − window_end, în minute de event-time)\n'
          'Echivalent cu "avg. window delay" din Awad et al. 2019',
          fontweight='bold')
plt.ylabel('Window Delay (minute)')
plt.xlabel('Strategie')
for i, v in enumerate(df_main['Window Delay (min)']):
    ax.text(i, v + 0.3, f"{v:.1f} min", ha='center', fontweight='bold')
plt.tight_layout()
plt.savefig(os.path.join(RESULTS, 'TAXI_Window_Delay.png'), dpi=300)
print("✅ Salvat: TAXI_Window_Delay.png")

#  Graf 1b: True Latency wall-clock (dependent de viteza replay) 
plt.figure(figsize=(9, 6))
ax = sns.barplot(x='Label', y='True Latency (s)', hue='Label', data=df_main,
                 palette=palette_main, legend=False)
plt.title('Latența Wall-Clock — Dataset Taxi NYC\n'
          '(now - max(arrival_time), dependent de viteza replay — referință secundară)',
          fontweight='bold')
plt.ylabel('True Latency (s)')
plt.xlabel('Strategie')
for i, v in enumerate(df_main['True Latency (s)']):
    ax.text(i, v + 0.05, f"{v:.2f}s", ha='center', fontweight='bold')
plt.tight_layout()
plt.savefig(os.path.join(RESULTS, 'TAXI_True_Latency.png'), dpi=300)
print("✅ Salvat: TAXI_True_Latency.png")

# Graf 2: Completeness
plt.figure(figsize=(9, 6))
ax = sns.barplot(x='Label', y='Completeness (%)', hue='Label', data=df_main, palette=palette_main, legend=False)
plt.axhline(y=100, color='gray', linestyle='--', label='Baseline speculative (100%)')
plt.title('Completitudinea Datelor — Dataset Taxi NYC\n'
          '(% din cursele capturate față de strategia speculativă)',
          fontweight='bold')
plt.ylabel('Completitudine (%)')
plt.xlabel('Strategie')
plt.ylim(0, 115)
plt.legend()
for i, v in enumerate(df_main['Completeness (%)']):
    ax.text(i, v + 1, f"{v:.1f}%", ha='center', fontweight='bold')
plt.tight_layout()
plt.savefig(os.path.join(RESULTS, 'TAXI_Completeness.png'), dpi=300)
print("✅ Salvat: TAXI_Completeness.png")

#  Graf 3: Overhead 
plt.figure(figsize=(9, 6))
ax = sns.barplot(x='Label', y='Overhead (Updates)', hue='Label', data=df_main, palette=palette_main, legend=False)
plt.title('Costul Computațional — Dataset Taxi NYC\n'
          '(scrieri medii în DB per fereastră de timp)',
          fontweight='bold')
plt.ylabel('Număr mediu actualizări per fereastră')
plt.xlabel('Strategie')
for i, v in enumerate(df_main['Overhead (Updates)']):
    ax.text(i, v + 0.02, f"{v:.1f}x", ha='center', fontweight='bold')
plt.tight_layout()
plt.savefig(os.path.join(RESULTS, 'TAXI_Overhead.png'), dpi=300)
print("✅ Salvat: TAXI_Overhead.png")

#  Graf 4: Tradeoff True Latency vs Completeness 
plt.figure(figsize=(8, 6))
for _, row in df_main.iterrows():
    color = COLORS.get(row['Strategy'], '#95a5a6')
    plt.scatter(row['True Latency (s)'], row['Completeness (%)'], color=color, s=200, zorder=5)
    plt.annotate(row['Label'].replace('\n', ' '),
                 (row['True Latency (s)'], row['Completeness (%)']),
                 textcoords="offset points", xytext=(8, 4), fontsize=10)
plt.title('Tradeoff: Latență vs. Completitudine — Taxi NYC\n'
          '(colț stânga-sus = ideal: latență mică, completitudine mare)',
          fontweight='bold')
plt.xlabel('True Latency (s)  [mai mic = mai bine]')
plt.ylabel('Completitudine (%)  [mai mare = mai bine]')
plt.tight_layout()
plt.savefig(os.path.join(RESULTS, 'TAXI_Tradeoff.png'), dpi=300)
print("✅ Salvat: TAXI_Tradeoff.png")

#  Graf 5: Eroarea Statistică față de Speculative 
spec_avg_dist, spec_avg_dur = get_speculative_final_stats(client, S_SPECULATIVE, time_window)
print(f"\n📊 Speculative ground truth: avg_distance={spec_avg_dist:.3f} mi, avg_duration={spec_avg_dur:.3f} min")

if spec_avg_dist > 0 and spec_avg_dur > 0:
    df_main['Distance Error (%)'] = (df_main['Avg Distance (mi)'] - spec_avg_dist) / spec_avg_dist * 100
    df_main['Duration Error (%)'] = (df_main['Avg Duration (min)'] - spec_avg_dur) / spec_avg_dur * 100
    df_main.loc[df_main['Strategy'] == S_SPECULATIVE, 'Distance Error (%)'] = 0.0
    df_main.loc[df_main['Strategy'] == S_SPECULATIVE, 'Duration Error (%)'] = 0.0

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 6))

    for ax, col, ylabel, title in [
        (ax1, 'Distance Error (%)', 'Eroare relativă (%)', 'Eroare Distanță Medie vs. Speculative'),
        (ax2, 'Duration Error (%)', 'Eroare relativă (%)', 'Eroare Durată Medie vs. Speculative'),
    ]:
        values = df_main[col].tolist()
        bars = ax.bar(df_main['Label'], values, color=palette_main)
        ax.axhline(0, color='black', linewidth=1.0)
        ax.set_title(title, fontweight='bold')
        ax.set_ylabel(ylabel)
        ax.set_xlabel('')
        for i, v in enumerate(values):
            offset = 1.5 if v >= 0 else -4.5
            ax.text(i, v + offset, f"{v:+.1f}%", ha='center', fontweight='bold', fontsize=9)

    fig.suptitle('Eroarea Statistică față de Strategia Completă (Speculative) — Taxi NYC\n'
                 '(eroare negativă = subestimare cauzată de cursele OOO pierdute)',
                 fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS, 'TAXI_Data_Quality.png'), dpi=300)
    print("✅ Salvat: TAXI_Data_Quality.png")
else:
    print("⚠️ Date Speculative insuficiente — graficul de eroare sărit")


# SECȚIUNEA 2: Completeness în timp (serii temporale)

print("\n📈 Completeness în timp (adaptarea ADWIN)...")

def query_completeness_timeseries(influx_client, strategy, tw, bucket="2m"):
   
    time_clause = f"AND time > now() - {tw}" if tw else ""
    q = f"""
    SELECT max("event_count") as max_events
    FROM "taxi_metrics"
    WHERE "strategy" = '{strategy}' {time_clause}
    GROUP BY time({bucket}), "zone_id"
    FILL(none)
    """
    res = influx_client.query(q)
    rows = []
    for (_, tags), pts in res.items():
        zone_id = tags.get('zone_id', '') if tags else ''
        if zone_id == 'CELL_OUT':
            continue
        for p in pts:
            if p.get('max_events') is not None:
                rows.append({'time': p['time'], 'max_events': float(p['max_events'])})
    if not rows:
        return []
    tmp = pd.DataFrame(rows).groupby('time')['max_events'].mean()
    return [{'time': t, 'strategy': strategy, 'avg_events': v} for t, v in tmp.items()]

ts_comp_rows = []
for s in ALL_STRATEGIES:
    ts_comp_rows.extend(query_completeness_timeseries(client, s, time_window, ts_bucket))

if ts_comp_rows:
    ts_comp_df = pd.DataFrame(ts_comp_rows)
    ts_comp_df['time'] = pd.to_datetime(ts_comp_df['time'])

    spec_ts = ts_comp_df[ts_comp_df['strategy'] == S_SPECULATIVE][['time', 'avg_events']]
    spec_ts = spec_ts.rename(columns={'avg_events': 'spec_events'})
    ts_comp_df = ts_comp_df.merge(spec_ts, on='time', how='left')
    ts_comp_df['completeness'] = (ts_comp_df['avg_events'] / ts_comp_df['spec_events'] * 100).clip(upper=100)
    ts_comp_df = ts_comp_df[ts_comp_df['strategy'] != S_SPECULATIVE]

    plt.figure(figsize=(13, 6))
    for strat, grp in ts_comp_df.groupby('strategy'):
        color = COLORS.get(strat, '#95a5a6')
        label = LABELS.get(strat, strat).replace('\n', ' ')
        grp = grp.sort_values('time').dropna(subset=['completeness'])
        plt.plot(grp['time'], grp['completeness'], marker='o', color=color,
                 label=label, linewidth=2, markersize=5)

    plt.axhline(100, color='#2ecc71', linewidth=1.5, linestyle='--', label='Speculative (100%)')
    plt.ylabel('Completitudine (%)')
    plt.xlabel('Timp (UTC)')
    plt.ylim(0, 115)
    plt.title('Completitudinea în timp — adaptarea strategiilor\n'
              '(ADWIN ar trebui să converge spre completeness mai mare pe măsură ce ajustează slack-ul)',
              fontweight='bold')
    plt.legend(loc='lower right')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS, 'TAXI_Completeness_Timeseries.png'), dpi=300)
    print("✅ Salvat: TAXI_Completeness_Timeseries.png")
else:
    print("⚠️ Date insuficiente pentru serii temporale completeness.")


# SECȚIUNEA 3: Heatmap completeness per zonă NYC (grila 10×10)

print("\n🗺️  Heatmap completeness per zonă NYC...")

def query_zone_events(influx_client, strategy, tw):
    where_time = f"AND time > now() - {tw}" if tw else ""
    q = f"""
    SELECT mean("event_count") as avg_events
    FROM "taxi_metrics"
    WHERE "strategy" = '{strategy}' {where_time}
    GROUP BY "zone_id"
    """
    res = influx_client.query(q)
    zone_map = {}
    for (_, tags), pts in res.items():
        zone_id = tags.get('zone_id', '') if tags else ''
        if not zone_id or zone_id == 'CELL_OUT':
            continue
        for p in pts:
            if p.get('avg_events') is not None:
                zone_map[zone_id] = float(p['avg_events'])
    return zone_map

def zone_to_grid(zone_id):
    parts = zone_id.replace('CELL_', '').split('_')
    if len(parts) == 2:
        try:
            return int(parts[0]), int(parts[1])
        except ValueError:
            pass
    return None, None

spec_zones = query_zone_events_speculative(client, S_SPECULATIVE, time_window)
strats_hm  = [S_NAIVE, S_WATERMARK, S_ADAPTIVE, S_REAL_ADWIN]
hm_labels  = [LABELS.get(s, s).replace('\n', ' ') for s in strats_hm]

all_zones = set(spec_zones.keys())
if all_zones:
    fig, axes = plt.subplots(1, len(strats_hm), figsize=(16, 5))
    if len(strats_hm) == 1:
        axes = [axes]

    for ax, strat, lbl in zip(axes, strats_hm, hm_labels):
        strat_zones = query_zone_events(client, strat, time_window)
        grid = [[float('nan')] * 10 for _ in range(10)]
        for zone_id, spec_ev in spec_zones.items():
            cx, cy = zone_to_grid(zone_id)
            if cx is None or not (0 <= cx < 10 and 0 <= cy < 10):
                continue
            strat_ev = strat_zones.get(zone_id, 0)
            completeness = min(100.0, strat_ev / spec_ev * 100) if spec_ev > 0 else 0
            grid[9 - cy][cx] = completeness  # y inverted (north = top)

        grid_arr = np.array(grid, dtype=float)
        im = ax.imshow(grid_arr, vmin=0, vmax=100, cmap='RdYlGn', aspect='auto')
        ax.set_title(lbl, fontweight='bold', fontsize=10)
        ax.set_xlabel('X (W→E)')
        ax.set_ylabel('Y (S→N)')
        ax.set_xticks(range(10))
        ax.set_yticks(range(10))
        ax.set_yticklabels(range(9, -1, -1))

    plt.colorbar(im, ax=axes, label='Completitudine (%)', shrink=0.8)
    fig.suptitle('Completitudine per Celulă NYC (grila 10×10) — față de Speculative\n'
                 '(verde=completă, roșu=multe curse pierdute)',
                 fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS, 'TAXI_Heatmap_Zones.png'), dpi=300)
    print("✅ Salvat: TAXI_Heatmap_Zones.png")
else:
    print("⚠️ Date insuficiente pentru heatmap zone.")


# SECȚIUNEA 4: Heatmap eroare statistică per zonă NYC (grila 10×10)

print("\n🗺️  Heatmap eroare statistică per zonă NYC...")

def query_zone_final_stats_speculative(influx_client, strategy, tw):
    """Per-zone avg_distance and avg_duration using final (max event_count) update per window."""
    raw = query_per_window_data(influx_client, strategy, tw)
    if raw.empty:
        return {}
    final = (raw.sort_values('event_count')
                .drop_duplicates(['window_end', 'zone_id'], keep='last'))
    result = {}
    for zone_id, grp in final.groupby('zone_id'):
        result[zone_id] = {
            'avg_distance': grp['avg_distance'].mean(),
            'avg_duration': grp['avg_duration'].mean(),
        }
    return result

def query_zone_stats_simple(influx_client, strategy, tw):
    """Per-zone avg_distance and avg_duration for non-speculative strategies (simple mean)."""
    where_time = f"AND time > now() - {tw}" if tw else ""
    q = f"""
    SELECT mean("avg_distance") as avg_distance, mean("avg_duration") as avg_duration
    FROM "taxi_metrics"
    WHERE "strategy" = '{strategy}' {where_time}
    GROUP BY "zone_id"
    """
    res = influx_client.query(q)
    zone_stats = {}
    for (_, tags), pts in res.items():
        zone_id = tags.get('zone_id', '') if tags else ''
        if not zone_id or zone_id == 'CELL_OUT':
            continue
        for p in pts:
            if p.get('avg_distance') is not None:
                zone_stats[zone_id] = {
                    'avg_distance': float(p.get('avg_distance') or 0),
                    'avg_duration': float(p.get('avg_duration') or 0),
                }
    return zone_stats

spec_zone_stats = query_zone_final_stats_speculative(client, S_SPECULATIVE, time_window)

if spec_zone_stats:
    metrics = [
        ('avg_distance', 'Eroare\nDistanță (%)'),
        ('avg_duration', 'Eroare\nDurată (%)'),
    ]
    fig, axes = plt.subplots(len(metrics), len(strats_hm), figsize=(16, 9))

    im_ref = None
    for col_idx, (strat, lbl) in enumerate(zip(strats_hm, hm_labels)):
        strat_zone_stats = query_zone_stats_simple(client, strat, time_window)
        for row_idx, (metric, metric_label) in enumerate(metrics):
            ax = axes[row_idx][col_idx]
            grid = [[float('nan')] * 10 for _ in range(10)]
            for zone_id, spec_s in spec_zone_stats.items():
                cx, cy = zone_to_grid(zone_id)
                if cx is None or not (0 <= cx < 10 and 0 <= cy < 10):
                    continue
                spec_val = spec_s[metric]
                strat_val = strat_zone_stats.get(zone_id, {}).get(metric, None)
                if strat_val is not None and spec_val > 0:
                    error_pct = abs(strat_val - spec_val) / spec_val * 100
                else:
                    error_pct = 100.0
                grid[9 - cy][cx] = min(100.0, error_pct)
            grid_arr = np.array(grid, dtype=float)
            im = ax.imshow(grid_arr, vmin=0, vmax=100, cmap='RdYlGn_r', aspect='auto')
            im_ref = im
            if row_idx == 0:
                ax.set_title(lbl, fontweight='bold', fontsize=9)
            if col_idx == 0:
                ax.set_ylabel(f'{metric_label}\nY (S→N)', fontsize=9)
            else:
                ax.set_ylabel('')
            ax.set_xticks(range(10))
            ax.set_yticks(range(10))
            ax.set_yticklabels(range(9, -1, -1), fontsize=7)
            ax.set_xticklabels(range(10), fontsize=7)
            if row_idx == len(metrics) - 1:
                ax.set_xlabel('X (W→E)', fontsize=8)

    if im_ref is not None:
        fig.colorbar(im_ref, ax=axes.ravel().tolist(), label='Eroare relativă absolută (%)', shrink=0.6)
    fig.suptitle('Eroarea Statistică per Celulă NYC (grila 10×10) — față de Speculative\n'
                 '(verde=eroare mică, roșu=eroare mare; rând 1=distanță medie, rând 2=durată medie)',
                 fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS, 'TAXI_Heatmap_Error_Zones.png'), dpi=300)
    print("✅ Salvat: TAXI_Heatmap_Error_Zones.png")
else:
    print("⚠️ Date Speculative insuficiente pentru heatmap eroare zone.")

plt.show()
print("\n🎉 Toate graficele taxi generate!")
