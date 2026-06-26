import pandas as pd
import os

INPUT  = r"C:\Users\carpr\Disertatie\disertatie-iot\data\trip_data\trip_data_1.csv"
OUTPUT = r"C:\Users\carpr\Disertatie\disertatie-iot\data\trip_data\trip_data_1_sorted.csv"

print("Citesc CSV...")
df = pd.read_csv(INPUT, dtype=str)

print(f"Rânduri citite: {len(df):,}")
print("Sortez după pickup_datetime...")

df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"].str.strip())
df.sort_values("pickup_datetime", inplace=True)
df["pickup_datetime"] = df["pickup_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

print(f"Salvez în {OUTPUT} ...")
df.to_csv(OUTPUT, index=False)

size_mb = os.path.getsize(OUTPUT) / 1024 / 1024
print(f"Done! {size_mb:.0f} MB -> {OUTPUT}")
print(f"Primul pickup: {df['pickup_datetime'].iloc[0]}")
print(f"Ultimul pickup: {df['pickup_datetime'].iloc[-1]}")
