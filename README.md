# An Empirical Comparison of Out-of-Order Event Management Strategies in Apache Flink

Master's thesis — UAIC Faculty of Informatics, 2026.  
**Author:** Răzvan Nicolae Carp  
**Coordinator:** Conf. Dr. Emanuel Onica

## Overview

This project implements and evaluates five out-of-order (OOO) event handling strategies in Apache Flink:

| Strategy | Description |
|---|---|
| **Naive** | No OOO handling — late events are discarded |
| **Fixed Watermark** | Static slack (configurable) |
| **Heuristic Adaptive Watermark** | ADWIN-based drift detection adjusts slack dynamically |
| **Official ADWIN** | Implements Awad et al. (2019) algorithm |
| **Speculative** | Emits early results via CountTrigger, then corrects on watermark |

Evaluated on two datasets: **SmartHome** (synthetic IoT, 100 houses, 3 network jitter regimes) and **NYC Taxi DEBS 2015** (real OOO from trip duration).

## Repository structure

```
generator/          — Go event generator (Kafka producer, 3 network regimes)
flink-processor/    — Apache Flink jobs in Java (one per strategy)
scripts/            — Python analysis scripts (InfluxDB queries, plots, CSVs)
data/               — Input CSV traces (SmartHome HomeC.csv, NYC Taxi trip_data/)
results/            — Experiment outputs (PNG plots, CSV exports)
docker-compose.yml  — Infrastructure (Kafka, ZooKeeper, InfluxDB, Grafana)
```

## Quick start

**Prerequisites:** Docker, Java 11+, Maven, Go 1.21+, Python 3.9+

```bash
# 1. Start infrastructure
docker-compose up -d

# 2. Build Flink jobs
cd flink-processor && mvn package

# 3. Run generator (interactive — choose dataset and network scenario)
cd generator && go run main.go

# 4. Submit a Flink job (example: fixed watermark strategy)
flink run flink-processor/target/flink-processor-0.1.jar com.smartcity.DataStreamJob

# 5. Analyze results
python scripts/final_comparison.py       # SmartHome comparison plots
python scripts/taxi_comparison.py        # Taxi comparison plots
python scripts/parameter_sensitivity.py  # SmartHome parameter sensitivity
python scripts/taxi_sensitivity.py       # Taxi parameter sensitivity
```

## Flink jobs

| Class | Strategy |
|---|---|
| `DataStreamJob` | Fixed Watermark (SmartHome) |
| `NaiveJob` | Naive (SmartHome) |
| `AdaptiveAdwinJob` | Heuristic Adaptive Watermark (SmartHome) |
| `RealAdwinJob` | Official ADWIN — Awad et al. 2019 (SmartHome) |
| `SpeculativeJob` | Speculative (SmartHome) |
| `TaxiDataStreamJob` | Fixed Watermark (Taxi) |
| `TaxiNaiveJob` | Naive (Taxi) |
| `TaxiAdaptiveAdwinJob` | Heuristic Adaptive Watermark (Taxi) |
| `TaxiRealAdwinJob` | Official ADWIN (Taxi) |
| `TaxiSpeculativeJob` | Speculative (Taxi) |
| `TaxiInOrderJob` | In-order baseline (Taxi) |

## License

Educational use.
