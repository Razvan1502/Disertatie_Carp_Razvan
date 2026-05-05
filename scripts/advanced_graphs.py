from influxdb import InfluxDBClient
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

print("🔍 Conectare la InfluxDB...")
client = InfluxDBClient(host='localhost', port=8086, database='smartcity')

RESULTS = r'C:\Users\carpr\Disertatie\disertatie-iot\results'
COLORS = {
    'watermark':           '#e74c3c',
    'adaptive_real_adwin': '#f39c12',
    'speculative':         '#2ecc71',
    'adaptive_adwin':      '#3498db',
}
sns.set_theme(style="whitegrid", context="paper", font_scale=1.2)

# =========================================================
# DATE BRUTE — pentru Box Plot si CDF
# =========================================================
print("📥 Extragere date brute...")
query_raw = """
SELECT "decisional_latency_ms", "true_latency_ms", "strategy"
FROM "house_metrics"
WHERE "house_id" !~ /-initial$/
"""
result_raw = client.query(query_raw)

rows = []
for (measurement, tags), points in result_raw.items():
    for point in points:
        if point.get('decisional_latency_ms') is not None and point.get('true_latency_ms') is not None:
            rows.append({
                'Strategy':                point['strategy'],
                'Decisional Latency (s)':  point['decisional_latency_ms'] / 1000.0,
                'True Latency (s)':        point['true_latency_ms'] / 1000.0,
            })

df_raw = pd.DataFrame(rows)

if df_raw.empty:
    print("❌ Nu s-au găsit date brute. Rulează job-urile Flink mai întâi.")
    exit()

print(f"✅ {len(df_raw)} înregistrări brute extrase.")
print(f"   Strategii găsite: {df_raw['Strategy'].unique()}")

# Ordinea fixă pe grafice
strategy_order = ['watermark', 'adaptive_adwin', 'adaptive_real_adwin', 'speculative']
strategy_order = [s for s in strategy_order if s in df_raw['Strategy'].unique()]
palette = [COLORS.get(s, '#95a5a6') for s in strategy_order]

# =========================================================
# GRAFICUL 1: Box Plot — Distribuția Latenței Decizionale
# =========================================================
plt.figure(figsize=(10, 6))
ax = sns.boxplot(
    x='Strategy', y='Decisional Latency (s)',
    data=df_raw, order=strategy_order, palette=palette,
    flierprops=dict(marker='o', markersize=2, alpha=0.4)
)
plt.axhline(y=0, color='black', linewidth=0.9, linestyle='--', alpha=0.6,
            label='Limita fereastră (t=0)')
plt.title('Distribuția Latenței Decizionale per Strategie\n'
          '(mediană, quartile, outlieri)', fontweight='bold')
plt.ylabel('Latență Decizională (s)  [negativ = rezultat anticipat]')
plt.xlabel('Strategie')
plt.legend()
plt.tight_layout()
plt.savefig(f'{RESULTS}\\ADV_Boxplot_Decisional.png', dpi=300)
print("✅ Salvat: ADV_Boxplot_Decisional.png")

# =========================================================
# GRAFICUL 2: Box Plot — Distribuția Latenței Reale
# =========================================================
plt.figure(figsize=(10, 6))
sns.boxplot(
    x='Strategy', y='True Latency (s)',
    data=df_raw, order=strategy_order, palette=palette,
    flierprops=dict(marker='o', markersize=2, alpha=0.4)
)
plt.title('Distribuția Latenței Reale de Procesare per Strategie\n'
          '(timp de la ultimul pachet primit până la emiterea rezultatului)',
          fontweight='bold')
plt.ylabel('Latență Reală (s)')
plt.xlabel('Strategie')
plt.tight_layout()
plt.savefig(f'{RESULTS}\\ADV_Boxplot_True.png', dpi=300)
print("✅ Salvat: ADV_Boxplot_True.png")

# =========================================================
# GRAFICUL 3: CDF — Latența Reală
# =========================================================
plt.figure(figsize=(10, 6))
for strategy in strategy_order:
    subset = df_raw[df_raw['Strategy'] == strategy]['True Latency (s)'].sort_values()
    cdf = pd.Series(range(1, len(subset) + 1), index=subset) / len(subset)
    plt.plot(cdf.index, cdf.values,
             label=strategy, color=COLORS.get(strategy, '#95a5a6'), linewidth=2)

plt.axhline(y=0.95, color='gray', linestyle=':', linewidth=1, label='Percentila 95%')
plt.title('CDF — Latența Reală de Procesare\n'
          '(ce % din ferestre au latența sub X secunde)', fontweight='bold')
plt.xlabel('Latență Reală (s)')
plt.ylabel('Probabilitate Cumulativă')
plt.legend(title='Strategie')
plt.gca().yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
plt.tight_layout()
plt.savefig(f'{RESULTS}\\ADV_CDF_True.png', dpi=300)
print("✅ Salvat: ADV_CDF_True.png")

# =========================================================
# DATE TIME SERIES — evolutie in timp
# =========================================================
print("📥 Extragere date time series...")
query_ts = """
SELECT
    mean("decisional_latency_ms") as avg_decisional,
    mean("true_latency_ms")       as avg_true
FROM "house_metrics"
WHERE "house_id" !~ /-initial$/
GROUP BY time(30s), "strategy"
"""
result_ts = client.query(query_ts)

ts_rows = []
for (measurement, tags), points in result_ts.items():
    strategy = tags['strategy']
    for point in points:
        if point.get('avg_decisional') is not None and point.get('avg_true') is not None:
            ts_rows.append({
                'time':               pd.to_datetime(point['time']),
                'Strategy':           strategy,
                'Decisional (s)':     point['avg_decisional'] / 1000.0,
                'True Latency (s)':   point['avg_true'] / 1000.0,
            })

df_ts = pd.DataFrame(ts_rows)

if df_ts.empty:
    print("⚠️  Nu s-au găsit date time series. Graficele 4 și 5 sunt omise.")
else:
    df_ts = df_ts.sort_values('time')

    # =========================================================
    # GRAFICUL 4: Time Series — Latența Decizională în Timp
    # =========================================================
    plt.figure(figsize=(12, 6))
    for strategy in strategy_order:
        subset = df_ts[df_ts['Strategy'] == strategy]
        if not subset.empty:
            plt.plot(subset['time'], subset['Decisional (s)'],
                     label=strategy, color=COLORS.get(strategy, '#95a5a6'),
                     linewidth=1.8, alpha=0.85)
    plt.axhline(y=0, color='black', linewidth=0.8, linestyle='--', alpha=0.5)
    plt.title('Evoluția Latenței Decizionale în Timp\n'
              '(arată cum strategiile adaptive se ajustează la schimbările de rețea)',
              fontweight='bold')
    plt.xlabel('Timp')
    plt.ylabel('Latență Decizională (s)')
    plt.legend(title='Strategie')
    plt.gcf().autofmt_xdate()
    plt.tight_layout()
    plt.savefig(f'{RESULTS}\\ADV_TimeSeries_Decisional.png', dpi=300)
    print("✅ Salvat: ADV_TimeSeries_Decisional.png")

    # =========================================================
    # GRAFICUL 5: Time Series — Latența Reală în Timp
    # =========================================================
    plt.figure(figsize=(12, 6))
    for strategy in strategy_order:
        subset = df_ts[df_ts['Strategy'] == strategy]
        if not subset.empty:
            plt.plot(subset['time'], subset['True Latency (s)'],
                     label=strategy, color=COLORS.get(strategy, '#95a5a6'),
                     linewidth=1.8, alpha=0.85)
    plt.title('Evoluția Latenței Reale de Procesare în Timp\n'
              '(arată stabilitatea fiecărei strategii)',
              fontweight='bold')
    plt.xlabel('Timp')
    plt.ylabel('Latență Reală (s)')
    plt.legend(title='Strategie')
    plt.gcf().autofmt_xdate()
    plt.tight_layout()
    plt.savefig(f'{RESULTS}\\ADV_TimeSeries_True.png', dpi=300)
    print("✅ Salvat: ADV_TimeSeries_True.png")

print("\n✅ Toate graficele avansate au fost generate!")
plt.show()
