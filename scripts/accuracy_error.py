from influxdb import InfluxDBClient
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

print("🔍 Conectare la InfluxDB...")
# Ne conectăm la baza de date din Docker
client = InfluxDBClient(host='localhost', port=8086, database='smartcity')

# 1. Extragem PRIMA predicție (Initial Guess)
query_initial = "SELECT mean(avg_temp) as temp_initial FROM house_metrics WHERE strategy='speculative' AND house_id =~ /-initial$/ GROUP BY time(10s), house_id"
result_initial = client.query(query_initial)
# Curățăm tag-ul "-initial" ca să le putem uni
data_initial = []
for (measurement, tags), points in result_initial.items():
    house_real = tags['house_id'].replace('-initial', '')
    for point in points:
        if point['temp_initial'] is not None:
            data_initial.append({'time': point['time'], 'house_id': house_real, 'temp_initial': point['temp_initial']})

df_initial = pd.DataFrame(data_initial)

# 2. Extragem valoarea FINALĂ (Ground Truth)
query_final = "SELECT mean(avg_temp) as temp_final FROM house_metrics WHERE strategy='speculative' AND house_id !~ /-initial$/ GROUP BY time(10s), house_id"
result_final = client.query(query_final)
data_final = []
for (measurement, tags), points in result_final.items():
    for point in points:
        if point['temp_final'] is not None:
            data_final.append({'time': point['time'], 'house_id': tags['house_id'], 'temp_final': point['temp_final']})

df_final = pd.DataFrame(data_final)

if df_initial.empty or df_final.empty:
    print("❌ Nu s-au găsit date. Asigură-te că SpeculativeJob a rulat cu noua modificare.")
    exit()

# 3. Combinăm cele două tabele după Timp și Casă
df_merge = pd.merge(df_initial, df_final, on=['time', 'house_id'])

# 4. CALCULĂM STATISTICILE DE EROARE (MATEMATICA PENTRU DISERTAȚIE)
df_merge['AbsoluteError'] = abs(df_merge['temp_initial'] - df_merge['temp_final'])
# Evităm împărțirea la zero
df_merge['PercentageError'] = (df_merge['AbsoluteError'] / df_merge['temp_final'].replace({0: 1})) * 100

mean_abs_error = df_merge['AbsoluteError'].mean()
mean_perc_error = df_merge['PercentageError'].mean()
max_error = df_merge['AbsoluteError'].max()

print("\n" + "="*50)
print("📊 STATISTICI ACURATEȚE (SPECULATIVE EXECUTION)")
print("="*50)
print(f"Număr total de ferestre analizate: {len(df_merge)}")
print(f"Eroarea Medie Absolută (MAE): {mean_abs_error:.3f}°C")
print(f"Eroarea Medie Procentuală (MAPE): {mean_perc_error:.3f}%")
print(f"Eroarea Maximă înregistrată: {max_error:.3f}°C")
print("="*50)

# 5. Generăm Graficul (Histograma Erorilor)
sns.set_theme(style="whitegrid")
plt.figure(figsize=(10, 6))

sns.histplot(df_merge['AbsoluteError'], bins=30, kde=True, color='#9b59b6')

plt.title('Distribuția Marjei de Eroare pentru Predicțiile Inițiale', fontsize=14, fontweight='bold')
plt.xlabel('Eroare Absolută (°C) între Prima Predicție și Rezultatul Final', fontsize=12)
plt.ylabel('Număr de Ferestre', fontsize=12)

# Adăugăm statisticile direct pe grafic
stats_text = f"Eroare Medie: {mean_abs_error:.3f}°C\nEroare Max: {max_error:.3f}°C"
plt.figtext(0.70, 0.75, stats_text, bbox=dict(facecolor='white', alpha=0.8, edgecolor='gray'), fontsize=12)

plt.tight_layout()
plt.savefig(r'C:\Users\carpr\Disertatie\disertatie-iot\results\speculative_error_margin.png')
print("\n✅ Graficul a fost salvat: results/speculative_error_margin.png")

plt.show()