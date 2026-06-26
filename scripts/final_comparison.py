from influxdb import InfluxDBClient
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime

run_name = input("Nume rulare (ex: smarthome_haos, smarthome_faze_scurte): ").strip()
if not run_name:
    run_name = "smarthome_run"
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
folder_name = f"{run_name}_{timestamp}"

RESULTS = os.path.join(r'C:\Users\carpr\Disertatie\disertatie-iot\results', folder_name)
os.makedirs(RESULTS, exist_ok=True)
print(f"📁 Rezultate salvate în: {RESULTS}\n")

time_window = "" 
print("→ Fereastra: toate datele din DB")

print("\nScenario rețea rulat:")
print("  1 - Haos Random (distribuție staționară)")
print("  2 - Faze Scurte (ciclu 6 min: 2min Excelent → 2min Congestie → 2min Recuperare)")
print("  3 - Faze Lungi  (ciclu 45 min: 15min Excelent → 15min Congestie → 15min Recuperare)")
raw_scen = input("Scenariu (1/2/3 — implicit 1): ").strip()
SCENARIO_LABELS = {
    '1': 'Haos Random',
    '2': 'Faze Scurte (ciclu 6 min)',
    '3': 'Faze Lungi (ciclu 45 min)',
}
TS_BUCKETS = {'1': '1m', '2': '1m', '3': '10m'}
scenario_name = SCENARIO_LABELS.get(raw_scen, 'Haos Random')
ts_bucket     = TS_BUCKETS.get(raw_scen, '1m')
print(f"→ Scenariu: {scenario_name} | Bucket timeseries: {ts_bucket}")

ALL_STRATEGIES = ['naive', 'watermark_5s', 'adaptive_adwin_dt1000', 'adaptive_real_adwin_L10_dd10', 'speculative']
LABELS = {
    'naive':                         'Naiv\n(fără OOO)',
    'watermark_5s':                  'Watermark Fix\n(5s)',
    'adaptive_adwin_dt1000':         'Heuristic Adaptive\nWatermark',
    'adaptive_real_adwin_L10_dd10':  'Official ADWIN\n(Awad 2019)',
    'speculative':                   'Speculative',
}
COLORS = {
    'naive':                         '#7f8c8d',
    'watermark_5s':                  '#e74c3c',
    'adaptive_real_adwin_L10_dd10':  '#f39c12',
    'speculative':                   '#2ecc71',
    'adaptive_adwin_dt1000':         '#3498db',
}

print("🔍 Conectare la InfluxDB...")
client = InfluxDBClient(host='localhost', port=8086, database='smartcity')

sns.set_theme(style="whitegrid", context="paper", font_scale=1.2)

def query_per_window_data(influx_client, strategy, tw):
    """Returnează toate înregistrările per (window_end, house_id), excluzând -initial.
    window_end NU e field în InfluxDB — e timestamp-ul scrierii. Folosim p['time'].
    """
    where_time = f"AND time > now() - {tw}" if tw else ""
    q = f"""
    SELECT "pachete_count", "avg_temp", "avg_energy", "update_count"
    FROM "house_metrics"
    WHERE "strategy" = '{strategy}' AND "house_id" !~ /-initial$/ {where_time}
    GROUP BY "house_id"
    """
    res = influx_client.query(q)
    rows = []
    for (_, tags), pts in res.items():
        house_id = tags.get('house_id', '')
        for p in pts:
            if p.get('pachete_count') is not None:
                rows.append({
                    'window_end':    p['time'], 
                    'house_id':      house_id,
                    'pachete_count': int(p.get('pachete_count') or 0),
                    'avg_temp':      float(p.get('avg_temp') or 0),
                    'avg_energy':    float(p.get('avg_energy') or 0),
                    'update_count':  int(p.get('update_count') or 1),
                })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def get_speculative_true_baseline(influx_client, tw):
    """Media FINALĂ a pachete_count per (window_end, house_id) pt Speculative.
    Ia max per fereastră (ultimul update = cel mai complet), evitând distorsiunea
    din update-urile intermediare ale CountTrigger.
    """
    raw = query_per_window_data(influx_client, 'speculative', tw)
    if raw.empty:
        return 0.0
    final = (raw.sort_values('pachete_count')
               .drop_duplicates(['window_end', 'house_id'], keep='last'))
    return final['pachete_count'].mean()


def query_house_events_speculative(influx_client, tw):
    """Media finală pachete_count per house_id pentru Speculative (baseline heatmap)."""
    raw = query_per_window_data(influx_client, 'speculative', tw)
    if raw.empty:
        return {}
    final = (raw.sort_values('pachete_count')
               .drop_duplicates(['window_end', 'house_id'], keep='last'))
    return final.groupby('house_id')['pachete_count'].mean().to_dict()


def query_completeness_timeseries(influx_client, strategy, tw, bucket):
    """mean(pachete_count) per (time_bucket, house_id) → media pe bucket.
    InfluxDB deduplicator pt Smart Home (timestamp=windowEnd) → un singur record per
    (window_end, house_id) → mean = final count. Nu mai e nevoie de max.
    """
    where_time = f"AND time > now() - {tw}" if tw else ""
    q = f"""
    SELECT mean("pachete_count") as mean_events
    FROM "house_metrics"
    WHERE "strategy" = '{strategy}' AND "house_id" !~ /-initial$/ {where_time}
    GROUP BY time({bucket}), "house_id"
    FILL(none)
    """
    res = influx_client.query(q)
    rows = []
    for (_, _), pts in res.items():
        for p in pts:
            if p.get('mean_events') is not None:
                rows.append({'time': p['time'], 'mean_events': float(p['mean_events'])})
    if not rows:
        return []
    tmp = pd.DataFrame(rows).groupby('time')['mean_events'].mean()
    return [{'time': t, 'strategy': strategy, 'avg_events': v} for t, v in tmp.items()]


def query_house_events(influx_client, strategy, tw):
    """mean(pachete_count) per house_id pentru heatmap (corect pt non-speculative)."""
    where_time = f"AND time > now() - {tw}" if tw else ""
    q = f"""
    SELECT mean("pachete_count") as avg_events
    FROM "house_metrics"
    WHERE "strategy" = '{strategy}' AND "house_id" !~ /-initial$/ {where_time}
    GROUP BY "house_id"
    """
    res = influx_client.query(q)
    house_map = {}
    for (_, tags), pts in res.items():
        hid = tags.get('house_id', '')
        for p in pts:
            if p.get('avg_events') is not None:
                house_map[hid] = float(p['avg_events'])
    return house_map


# --- Secțiunea 1: Statistici globale ---
print("\n🔍 Interogare statistici globale...")
strat_filter = " OR ".join([f'"strategy" = \'{s}\'' for s in ALL_STRATEGIES])
time_clause  = f"AND time > now() - {time_window}" if time_window else ""

query = f"""
SELECT
    mean("true_latency_ms")       as avg_true_latency,
    mean("decisional_latency_ms") as avg_decisional_latency,
    mean("pachete_count")         as avg_events,
    mean("update_count")          as avg_overhead
FROM "house_metrics"
WHERE ({strat_filter}) AND "house_id" !~ /-initial$/ {time_clause}
GROUP BY "strategy"
"""

result = client.query(query)
data = []
for (_, tags), points in result.items():
    strategy = tags['strategy']
    for point in points:
        data.append({
            'Strategy':               strategy,
            'True Latency (s)':       (point['avg_true_latency'] or 0) / 1000.0,
            'Decisional Latency (s)': (point['avg_decisional_latency'] or 0) / 1000.0,
            'Avg Events/Window':      point['avg_events'] or 0,
            'Overhead (Updates)':     point['avg_overhead'] or 0,
        })

if not data:
    print("❌ Nu am găsit date în house_metrics! Lasă job-urile să ruleze câteva minute.")
    exit()

df_main = pd.DataFrame(data)

spec_row = df_main[df_main['Strategy'] == 'speculative']
spec_events_true = get_speculative_true_baseline(client, time_window) if not spec_row.empty else 0.0
if spec_events_true > 0:
    df_main['Completeness (%)'] = (df_main['Avg Events/Window'] / spec_events_true * 100).clip(upper=100)
    df_main.loc[df_main['Strategy'] == 'speculative', 'Completeness (%)'] = 100.0
else:
    df_main['Completeness (%)'] = df_main['Avg Events/Window']

df_main['Label'] = df_main['Strategy'].map(lambda s: LABELS.get(s, s))
palette_main = [COLORS.get(s, '#95a5a6') for s in df_main['Strategy']]

print("\n✅ Date extrase:")
print(df_main[['Strategy', 'True Latency (s)', 'Decisional Latency (s)',
               'Avg Events/Window', 'Completeness (%)', 'Overhead (Updates)']].to_string(index=False))

# Graf 1: True Latency
plt.figure(figsize=(9, 6))
ax = sns.barplot(x='Label', y='True Latency (s)', hue='Label', data=df_main,
                 palette=palette_main, legend=False)
plt.title(f'Latența Reală de Procesare — Smart Home\n(Scenariu: {scenario_name})',
          fontweight='bold')
plt.ylabel('True Latency (s)')
plt.xlabel('Strategie')
for i, v in enumerate(df_main['True Latency (s)']):
    ax.text(i, v + 0.05, f"{v:.2f}s", ha='center', fontweight='bold')
plt.tight_layout()
plt.savefig(os.path.join(RESULTS, 'SH_True_Latency.png'), dpi=300)
print("✅ Salvat: SH_True_Latency.png")

# Graf 2: Decisional Latency
plt.figure(figsize=(9, 6))
ax = sns.barplot(x='Label', y='Decisional Latency (s)', hue='Label', data=df_main,
                 palette=palette_main, legend=False)
plt.axhline(y=0, color='black', linewidth=0.8, linestyle='--', alpha=0.6)
plt.title(f'Latența Decizională — Smart Home\n'
          f'(negativ = rezultat emis înainte de închiderea ferestrei de timp)',
          fontweight='bold')
plt.ylabel('Decisional Latency (s)')
plt.xlabel('Strategie')
for i, v in enumerate(df_main['Decisional Latency (s)']):
    offset = 0.1 if v >= 0 else -0.3
    ax.text(i, v + offset, f"{v:+.2f}s", ha='center', fontweight='bold')
plt.tight_layout()
plt.savefig(os.path.join(RESULTS, 'SH_Decisional_Latency.png'), dpi=300)
print("✅ Salvat: SH_Decisional_Latency.png")

# Graf 3: Completeness
plt.figure(figsize=(9, 6))
ax = sns.barplot(x='Label', y='Completeness (%)', hue='Label', data=df_main,
                 palette=palette_main, legend=False)
plt.axhline(y=100, color='gray', linestyle='--', label='Speculative (100%)')
plt.title(f'Completitudinea Datelor — Smart Home\n(Scenariu: {scenario_name})',
          fontweight='bold')
plt.ylabel('Completitudine (%)')
plt.xlabel('Strategie')
plt.ylim(0, 115)
plt.legend()
for i, v in enumerate(df_main['Completeness (%)']):
    ax.text(i, v + 1, f"{v:.1f}%", ha='center', fontweight='bold')
plt.tight_layout()
plt.savefig(os.path.join(RESULTS, 'SH_Completeness.png'), dpi=300)
print("✅ Salvat: SH_Completeness.png")

# Graf 4: Overhead 
plt.figure(figsize=(9, 6))
ax = sns.barplot(x='Label', y='Overhead (Updates)', hue='Label', data=df_main,
                 palette=palette_main, legend=False)
plt.title(f'Costul Computațional — Smart Home\n(scrieri medii în DB per fereastră de 10s)',
          fontweight='bold')
plt.ylabel('Număr mediu actualizări per fereastră')
plt.xlabel('Strategie')
for i, v in enumerate(df_main['Overhead (Updates)']):
    ax.text(i, v + 0.02, f"{v:.1f}x", ha='center', fontweight='bold')
plt.tight_layout()
plt.savefig(os.path.join(RESULTS, 'SH_Overhead.png'), dpi=300)
print("✅ Salvat: SH_Overhead.png")

# Graf 5: Tradeoff True Latency vs Completeness
# Offset-uri per strategie pentru a evita suprapunerea label-urilor
LABEL_OFFSETS = {
    'naive':                         (8, -14),
    'watermark_5s':                  (8,   4),
    'adaptive_adwin_dt1000':         (8,   6),
    'adaptive_real_adwin_L10_dd10':  (8, -14),
    'speculative':                   (8,   4),
}

plt.figure(figsize=(9, 6))
for _, row in df_main.iterrows():
    color = COLORS.get(row['Strategy'], '#95a5a6')
    plt.scatter(row['True Latency (s)'], row['Completeness (%)'],
                color=color, s=200, zorder=5)
    offset = LABEL_OFFSETS.get(row['Strategy'], (8, 4))
    plt.annotate(row['Label'].replace('\n', ' '),
                 (row['True Latency (s)'], row['Completeness (%)']),
                 textcoords="offset points", xytext=offset, fontsize=10)
plt.title(f'Tradeoff: Latență Reală vs. Completitudine — Smart Home\n'
          f'(colț stânga-sus = ideal: latență mică, completitudine mare)',
          fontweight='bold')
plt.xlabel('True Latency (s)  [mai mic = mai bine]')
plt.ylabel('Completitudine (%)')
plt.tight_layout()
plt.savefig(os.path.join(RESULTS, 'SH_Tradeoff.png'), dpi=300)
print("✅ Salvat: SH_Tradeoff.png")



# SECȚIUNEA 2: Completeness în timp

print("\n📈 Completeness în timp...")

ts_rows = []
for s in ALL_STRATEGIES:
    ts_rows.extend(query_completeness_timeseries(client, s, time_window, ts_bucket))

if ts_rows:
    ts_df = pd.DataFrame(ts_rows)
    ts_df['time'] = pd.to_datetime(ts_df['time'])

    spec_ts = ts_df[ts_df['strategy'] == 'speculative'][['time', 'avg_events']]
    spec_ts = spec_ts.rename(columns={'avg_events': 'spec_events'})
    ts_df = ts_df.merge(spec_ts, on='time', how='left')
    ts_df['completeness'] = (ts_df['avg_events'] / ts_df['spec_events'] * 100).clip(upper=100)
    ts_df = ts_df[ts_df['strategy'] != 'speculative']

    plt.figure(figsize=(13, 6))
    for strat, grp in ts_df.groupby('strategy'):
        color = COLORS.get(strat, '#95a5a6')
        label = LABELS.get(strat, strat).replace('\n', ' ')
        grp = grp.sort_values('time').dropna(subset=['completeness'])
        plt.plot(grp['time'], grp['completeness'], marker='o', color=color,
                 label=label, linewidth=2, markersize=5)
    plt.axhline(100, color='#2ecc71', linewidth=1.5, linestyle='--', label='Speculative (100%)')
    plt.ylabel('Completitudine (%)')
    plt.xlabel('Timp (UTC)')
    plt.ylim(0, 115)
    plt.title(f'Completitudinea în timp — Smart Home | {scenario_name}\n'
              '(ADWIN ar trebui să converge spre 100% pe măsură ce ajustează slack-ul)',
              fontweight='bold')
    plt.legend(loc='lower right')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS, 'SH_Completeness_Timeseries.png'), dpi=300)
    print("✅ Salvat: SH_Completeness_Timeseries.png")
else:
    print("⚠️ Date insuficiente pentru serii temporale completeness.")


# SECȚIUNEA 3: Heatmap completeness per casă (grid 10×10, HOUSE_0..99)

print("\n🏠 Heatmap completeness per casă...")

spec_houses = query_house_events_speculative(client, time_window)
strats_hm   = [s for s in ALL_STRATEGIES if s != 'speculative']
hm_labels   = [LABELS.get(s, s).replace('\n', ' ') for s in strats_hm]

if spec_houses:
    fig, axes = plt.subplots(1, len(strats_hm), figsize=(16, 5))
    for ax, strat, lbl in zip(axes, strats_hm, hm_labels):
        strat_houses = query_house_events(client, strat, time_window)
        grid = np.full((10, 10), float('nan'))
        for hid, spec_ev in spec_houses.items():
            try:
                n = int(hid.replace('HOUSE_', ''))
                row_i, col_i = n // 10, n % 10
                strat_ev = strat_houses.get(hid, 0)
                completeness = min(100.0, strat_ev / spec_ev * 100) if spec_ev > 0 else 0
                grid[9 - row_i][col_i] = completeness
            except (ValueError, IndexError):
                continue
        im = ax.imshow(grid, vmin=0, vmax=100, cmap='RdYlGn', aspect='auto')
        ax.set_title(lbl, fontweight='bold', fontsize=10)
        ax.set_xlabel('Casă (coloană 0–9)')
        ax.set_ylabel('Casă (rând 0–9)')
        ax.set_xticks(range(10))
        ax.set_yticks(range(10))
        ax.set_yticklabels(range(9, -1, -1))

    plt.colorbar(im, ax=axes, label='Completitudine (%)', shrink=0.8)
    fig.suptitle(f'Completitudine per Casă (grid 10×10, HOUSE_0..99) — față de Speculative\n'
                 f'Scenariu: {scenario_name}',
                 fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS, 'SH_Heatmap_Houses.png'), dpi=300)
    print("✅ Salvat: SH_Heatmap_Houses.png")
else:
    print("⚠️ Date insuficiente pentru heatmap case.")


# SECȚIUNEA 4: Eroarea statistică față de Speculative (temperatură & energie)

print("\n📊 Eroarea statistică față de Speculative (temperatură & energie)...")

q_quality = f"""
SELECT mean("avg_temp") as mean_temp, mean("avg_energy") as mean_energy
FROM "house_metrics"
WHERE ({strat_filter}) AND "house_id" !~ /-initial$/ {time_clause}
GROUP BY "strategy"
"""
res_q = client.query(q_quality)
quality_data = []
for (_, tags), pts in res_q.items():
    strat = tags['strategy']
    for p in pts:
        if p.get('mean_temp') is not None:
            quality_data.append({
                'Strategy':         strat,
                'Label':            LABELS.get(strat, strat).replace('\n', ' '),
                'Avg Temp (°C)':    float(p['mean_temp']),
                'Avg Energy (kWh)': float(p.get('mean_energy') or 0),
            })

if quality_data:
    df_q = pd.DataFrame(quality_data)
    palette_q = [COLORS.get(s, '#95a5a6') for s in df_q['Strategy']]

    spec_q = df_q[df_q['Strategy'] == 'speculative']
    if not spec_q.empty:
        spec_temp   = spec_q['Avg Temp (°C)'].values[0]
        spec_energy = spec_q['Avg Energy (kWh)'].values[0]
        print(f"📊 Speculative ground truth: avg_temp={spec_temp:.4f}°C, avg_energy={spec_energy:.6f} kWh")

        if spec_temp > 0 and spec_energy > 0:
            df_q['Temp Error (%)']   = (df_q['Avg Temp (°C)']    - spec_temp)   / spec_temp   * 100
            df_q['Energy Error (%)'] = (df_q['Avg Energy (kWh)'] - spec_energy) / spec_energy * 100
            df_q.loc[df_q['Strategy'] == 'speculative', 'Temp Error (%)']   = 0.0
            df_q.loc[df_q['Strategy'] == 'speculative', 'Energy Error (%)'] = 0.0

            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 6))

            for ax, col, ylabel, title in [
                (ax1, 'Temp Error (%)',   'Eroare relativă (%)', 'Eroare Temperatură Medie vs. Speculative'),
                (ax2, 'Energy Error (%)', 'Eroare relativă (%)', 'Eroare Consum Mediu Energie vs. Speculative'),
            ]:
                values = df_q[col].tolist()
                ax.bar(df_q['Label'], values, color=palette_q)
                ax.axhline(0, color='black', linewidth=1.0)
                ax.set_title(title, fontweight='bold')
                ax.set_ylabel(ylabel)
                ax.set_xlabel('')
                ax.tick_params(axis='x', labelsize=9)
                plt.setp(ax.get_xticklabels(), rotation=20, ha='right')
                for i, v in enumerate(values):
                    offset = 0.05 if v >= 0 else -0.2
                    ax.text(i, v + offset, f"{v:+.2f}%", ha='center', fontweight='bold', fontsize=9)

            fig.suptitle(f'Eroarea Statistică față de Strategia Completă (Speculative) — Smart Home\n'
                         f'Scenariu: {scenario_name} | (eroare negativă = subestimare din cauza evenimentelor OOO pierdute)',
                         fontweight='bold')
            plt.tight_layout()
            plt.savefig(os.path.join(RESULTS, 'SH_Data_Quality.png'), dpi=300)
            print("✅ Salvat: SH_Data_Quality.png")
        else:
            print("⚠️ Valori Speculative zero — graficul de eroare sărit")
    else:
        print("⚠️ Date Speculative lipsă din query calitate — graficul de eroare sărit")
else:
    print("⚠️ Date insuficiente pentru calitatea datelor.")

plt.show()
print(f"\n🎉 Toate graficele Smart Home generate în: {RESULTS}")
