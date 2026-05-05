from influxdb import InfluxDBClient
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

print("🔍 Conectare la InfluxDB...")
client = InfluxDBClient(host='localhost', port=8086, database='smartcity')

query = """
SELECT
    mean("decisional_latency_ms") as avg_decisional_latency,
    mean("true_latency_ms")       as avg_true_latency,
    mean("pachete_count")         as avg_completeness,
    mean("update_count")          as avg_overhead
FROM "house_metrics"
WHERE "house_id" !~ /-initial$/
GROUP BY "strategy"
"""

result = client.query(query)

data = []
for (measurement, tags), points in result.items():
    strategy = tags['strategy']
    for point in points:
        data.append({
            'Strategy': strategy,
            'Decisional Latency (s)': point['avg_decisional_latency'] / 1000.0,
            'True Latency (s)':       point['avg_true_latency'] / 1000.0,
            'Completeness (%)':       (point['avg_completeness'] / 10.0) * 100.0,
            'Overhead (Updates)':     point['avg_overhead']
        })

if not data:
    print("❌ Nu am găsit date în InfluxDB!")
    exit()

df = pd.DataFrame(data)
print("✅ Date extrase:\n", df.to_string(index=False))

sns.set_theme(style="whitegrid", context="paper", font_scale=1.2)
COLORS = {
    'watermark':          '#e74c3c',
    'adaptive_real_adwin':'#f39c12',
    'speculative':        '#2ecc71',
    'adaptive_adwin':     '#3498db',
}
palette = [COLORS.get(s, '#95a5a6') for s in df['Strategy']]

RESULTS = r'C:\Users\carpr\Disertatie\disertatie-iot\results'

# =========================================================
# GRAFICUL 1: Latență Decizională (poate fi negativă)
# =========================================================
plt.figure(figsize=(9, 6))
ax = sns.barplot(x='Strategy', y='Decisional Latency (s)', data=df, palette=palette)
plt.axhline(y=0, color='black', linewidth=0.8, linestyle='--')
plt.title('Latența Decizională Medie per Strategie\n'
          '(negativ = rezultat anticipat față de închiderea ferestrei)',
          fontweight='bold')
plt.ylabel('Latență Decizională (s)')
plt.xlabel('Strategie')
for i, v in enumerate(df['Decisional Latency (s)']):
    offset = 0.15 if v >= 0 else -0.4
    ax.text(i, v + offset, f"{v:.2f}s", color='black', ha='center', fontweight='bold')
plt.tight_layout()
plt.savefig(f'{RESULTS}\\FINAL_Decisional_Latency.png', dpi=300)
print("✅ Salvat: FINAL_Decisional_Latency.png")

# =========================================================
# GRAFICUL 2: Latență Reală (True Latency — mereu >= 0)
# =========================================================
plt.figure(figsize=(9, 6))
ax = sns.barplot(x='Strategy', y='True Latency (s)', data=df, palette=palette)
plt.title('Latența Reală de Procesare per Strategie\n'
          '(timp de la ultimul pachet primit până la emiterea rezultatului)',
          fontweight='bold')
plt.ylabel('Latență Reală (s)')
plt.xlabel('Strategie')
for i, v in enumerate(df['True Latency (s)']):
    ax.text(i, v + 0.05, f"{v:.2f}s", color='black', ha='center', fontweight='bold')
plt.tight_layout()
plt.savefig(f'{RESULTS}\\FINAL_True_Latency.png', dpi=300)
print("✅ Salvat: FINAL_True_Latency.png")

# =========================================================
# GRAFICUL 3: Acuratețe (Completeness)
# =========================================================
plt.figure(figsize=(9, 6))
ax = sns.barplot(x='Strategy', y='Completeness (%)', data=df, palette=palette)
plt.axhline(y=100, color='gray', linestyle='--', label='Ideal (100%)')
plt.title('Completitudinea Datelor per Strategie\n(pachete procesate din fereastră)',
          fontweight='bold')
plt.ylabel('Completitudine (%)')
plt.xlabel('Strategie')
plt.ylim(0, 115)
plt.legend()
for i, v in enumerate(df['Completeness (%)']):
    ax.text(i, v + 1, f"{v:.1f}%", color='black', ha='center', fontweight='bold')
plt.tight_layout()
plt.savefig(f'{RESULTS}\\FINAL_Completeness.png', dpi=300)
print("✅ Salvat: FINAL_Completeness.png")

# =========================================================
# GRAFICUL 4: Overhead Computațional
# =========================================================
plt.figure(figsize=(9, 6))
ax = sns.barplot(x='Strategy', y='Overhead (Updates)', data=df, palette=palette)
plt.title('Costul Computațional per Strategie\n(scrieri medii în DB per fereastră)',
          fontweight='bold')
plt.ylabel('Număr mediu de actualizări per fereastră')
plt.xlabel('Strategie')
for i, v in enumerate(df['Overhead (Updates)']):
    ax.text(i, v + 0.05, f"{v:.1f}x", color='black', ha='center', fontweight='bold')
plt.tight_layout()
plt.savefig(f'{RESULTS}\\FINAL_Overhead.png', dpi=300)
print("✅ Salvat: FINAL_Overhead.png")

# =========================================================
# GRAFICUL 5: Tradeoff — Latență Decizională vs. Completitudine
# =========================================================
plt.figure(figsize=(8, 6))
for _, row in df.iterrows():
    color = COLORS.get(row['Strategy'], '#95a5a6')
    plt.scatter(row['Decisional Latency (s)'], row['Completeness (%)'],
                color=color, s=200, zorder=5)
    plt.annotate(row['Strategy'],
                 (row['Decisional Latency (s)'], row['Completeness (%)']),
                 textcoords="offset points", xytext=(8, 4), fontsize=10)
plt.axvline(x=0, color='black', linewidth=0.8, linestyle='--', alpha=0.5)
plt.title('Tradeoff: Latență Decizională vs. Completitudine\n'
          '(colț stânga-sus = ideal)', fontweight='bold')
plt.xlabel('Latență Decizională (s)  [negativ = anticipat]')
plt.ylabel('Completitudine (%)')
plt.tight_layout()
plt.savefig(f'{RESULTS}\\FINAL_Tradeoff.png', dpi=300)
print("✅ Salvat: FINAL_Tradeoff.png")

plt.show()
