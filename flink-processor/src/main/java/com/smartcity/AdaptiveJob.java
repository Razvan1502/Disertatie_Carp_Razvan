package com.smartcity;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;

public class AdaptiveJob {

    public static class AdaptiveWatermarkGenerator implements WatermarkGenerator<SmartHomeEvent> {
        private long maxTimestampSeen = Long.MIN_VALUE;
        private long lastEmittedWatermark = Long.MIN_VALUE;

        // START de la 5s (identic cu metoda statica)
        private long m = 5000;
        private final double l = 0.1;  // Tolerăm maxim 10% late arrivals

        private int totalElements = 0;
        private int lateElements = 0;
        private long currentMaxLateness = 0;

        @Override
        public void onEvent(SmartHomeEvent event, long eventTimestamp, WatermarkOutput output) {
            maxTimestampSeen = Math.max(maxTimestampSeen, eventTimestamp);
            totalElements++;

            // Măsurăm latența reală (jitter-ul din Go)
            long lateness = event.arrival_time - event.event_time;
            currentMaxLateness = Math.max(currentMaxLateness, lateness);

            // Un pachet e "late" dacă e mai mic decât pragul teoretic deja închis
            if (eventTimestamp < (maxTimestampSeen - m)) {
                lateElements++;
            }

            // Evaluăm Drift-ul la fiecare 500 de evenimente
            if (totalElements >= 500) {
                double lateRate = (double) lateElements / totalElements;

                if (lateRate > l) {
                    // REȚEA LENTĂ: Creștem m pentru a prinde datele (max 12s)
                    m = Math.min(12000, currentMaxLateness);
                    System.out.println(String.format("⚠️ DRIFT (Lent): Rata eroare %.2f. Cresc m la %.2fs", lateRate, m/1000.0));
                } else {
                    // REȚEA BUNĂ: Putem încerca să scădem m pentru viteză
                    // Scădem mai agresiv (1000ms) dacă nu avem erori deloc
                    long step = (lateElements == 0) ? 1000 : 500;
                    m = Math.max(1000, m - step);
                    System.out.println(String.format("✅ REȚEA OK: Rata eroare %.2f. Scad m la %.2fs", lateRate, m/1000.0));
                }

                // Resetăm pentru următorul lot
                totalElements = 0;
                lateElements = 0;
                currentMaxLateness = 0;
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            if (maxTimestampSeen != Long.MIN_VALUE) {
                long potentialWM = maxTimestampSeen - m;
                if (potentialWM > lastEmittedWatermark) {
                    lastEmittedWatermark = potentialWM;
                    output.emitWatermark(new Watermark(lastEmittedWatermark));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<SmartHomeEvent> source = KafkaSource.<SmartHomeEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("iot-sensor-data")
                .setGroupId("adaptive-final-v2-" + System.currentTimeMillis())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(SmartHomeEvent.class))
                .build();

        WatermarkStrategy<SmartHomeEvent> adaptiveStrategy = WatermarkStrategy
                .forGenerator((ctx) -> new AdaptiveWatermarkGenerator())
                .withTimestampAssigner((event, timestamp) -> event.event_time)
                .withIdleness(Duration.ofSeconds(1));

        DataStream<SmartHomeEvent> events = env.fromSource(source, adaptiveStrategy, "Kafka Ingest");

        DataStream<SmartHomeResult> results = events
                .keyBy(event -> event.house_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<SmartHomeEvent, SmartHomeResult, String, TimeWindow>() {
                    @Override
                    public void process(String houseId, Context context, Iterable<SmartHomeEvent> elements, Collector<SmartHomeResult> out) {
                        double sumTemp = 0; double sumEnergy = 0; long maxArrivalTime = 0; int pCount = 0;
                        for (SmartHomeEvent e : elements) {
                            sumTemp += e.temperature; sumEnergy += e.energy_usage;
                            maxArrivalTime = Math.max(maxArrivalTime, e.arrival_time);
                            pCount++;
                        }
                        long windowEnd = context.window().getEnd();
                        long now = System.currentTimeMillis();
                        long decisionalLatency = now - windowEnd;
                        long trueLatency = now - maxArrivalTime;

                        out.collect(new SmartHomeResult(houseId, windowEnd, decisionalLatency, trueLatency, pCount, sumTemp/pCount, sumEnergy/pCount, true, 1));
                    }
                });

        results.addSink(new InfluxDBSink("adaptive"));
        env.execute("Smart City - Official Adaptive Watermarking");
    }
}