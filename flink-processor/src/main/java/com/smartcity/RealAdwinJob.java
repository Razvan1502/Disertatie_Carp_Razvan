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

public class RealAdwinJob {

    public static class OfficialAdwinWatermarkGenerator implements WatermarkGenerator<SmartHomeEvent> {
        private long maxTimestampSeen = Long.MIN_VALUE;
        private long lastEmittedWatermark = Long.MIN_VALUE;

        // Parametrii Algorithm 1 (Awad et al. 2019)
        private long lateElements = 0;
        private long totalElements = 0;
        private double delta = 1.0;        // δ=1 implicit (maxim sensibil, conform paper)
        private final double L = 0.1;      // prag toleranță late arrivals
        private final double deltaStep = 0.1; // ∆δ

        // Warmup — inițializăm m din primele w tuple (Algorithm 1, liniile 7-9)
        // Folosim media în loc de max pentru a evita supra-estimarea din outlieri
        private final int WARMUP_COUNT = 500;
        private int warmupDone = 0;
        private long warmupSum = 0;
        private long currentSlack = 2000; // valoare de start până la finalizarea warmup-ului

        private final ADWIN adwin = new ADWIN(delta);

        @Override
        public void onEvent(SmartHomeEvent event, long eventTimestamp, WatermarkOutput output) {
            maxTimestampSeen = Math.max(maxTimestampSeen, eventTimestamp);

            long lateness = Math.max(0, event.arrival_time - event.event_time);

            // Faza warmup: m = medie(tp(e) - te(e)) + buffer (Algorithm 1, liniile 7-9)
            // Folosim media în loc de max: max-ul din chaos mode (12s) ar bloca orice adaptare
            if (warmupDone < WARMUP_COUNT) {
                warmupSum += lateness;
                warmupDone++;
                if (warmupDone == WARMUP_COUNT) {
                    long avgLateness = warmupSum / WARMUP_COUNT;
                    currentSlack = Math.min(8000, Math.max(1000, avgLateness + 1000));
                    System.out.printf("✅ ADWIN Warmup finalizat: avgLateness=%dms → currentSlack=%dms\n",
                            avgLateness, currentSlack);
                }
                return;
            }

            totalElements++;

            // FIX 2: Normalizare input la [0,1] conform paper (linia 12): (tp-te)/m
            double normalizedLateness = (currentSlack > 0) ? (double) lateness / currentSlack : 0.0;
            // Cap la 2.0 pentru outlieri extremi (lateness > 2×m)
            normalizedLateness = Math.min(normalizedLateness, 2.0);

            if (lastEmittedWatermark != Long.MIN_VALUE && eventTimestamp < lastEmittedWatermark) {
                lateElements++;
            }

            boolean isDriftDetected = adwin.setInput(normalizedLateness);

            if (isDriftDetected) {
                double currentLateRate = (totalElements > 0) ? (double) lateElements / totalElements : 0.0;

                if (lateElements == 0) {
                    // FIX 3: Creștem sensibilitatea când nu avem late arrivals (paper linia 14)
                    delta = Math.min(1.0, delta + deltaStep);
                    adwin.setDelta(delta);
                } else if (currentLateRate < L) {
                    // Actualizăm slack-ul: estimarea ADWIN e normalizată → denormalizăm
                    long newSlack = (long)(adwin.getEstimation() * currentSlack) + 500;
                    currentSlack = Math.min(12000, Math.max(1000, newSlack));

                    System.out.printf("✅ ADWIN DRIFT OK: lateRate=%.2f < L=%.2f → Slack ajustat la %dms, δ=%.2f\n",
                            currentLateRate, L, currentSlack, delta);

                    // FIX 4: Resetăm ambele contoare (Algorithm 1, liniile 18-19)
                    lateElements = 0;
                    totalElements = 0;
                } else {
                    // Scădem sensibilitatea când avem prea multe late arrivals (paper linia 22)
                    delta = Math.max(0.01, delta - deltaStep);
                    adwin.setDelta(delta);
                    System.out.printf("⚠️ ADWIN: lateRate=%.2f >= L=%.2f → Scad sensibilitatea δ=%.2f\n",
                            currentLateRate, L, delta);
                }
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // Nu emitem watermark în faza de warmup
            if (warmupDone < WARMUP_COUNT || maxTimestampSeen == Long.MIN_VALUE) return;

            long potentialWM = maxTimestampSeen - currentSlack;
            if (potentialWM > lastEmittedWatermark) {
                lastEmittedWatermark = potentialWM;
                output.emitWatermark(new Watermark(lastEmittedWatermark));
            }
        }
    }


    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        String uniqueGroupId = "real-adwin-group-" + System.currentTimeMillis();
        KafkaSource<SmartHomeEvent> source = KafkaSource.<SmartHomeEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("iot-sensor-data")
                .setGroupId(uniqueGroupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(SmartHomeEvent.class))
                .build();

        //  Aplicarea strategiei ADWIN Matematic (Statistical Adaptive)
        WatermarkStrategy<SmartHomeEvent> adwinStrategy = WatermarkStrategy
                .forGenerator((ctx) -> new OfficialAdwinWatermarkGenerator())
                .withTimestampAssigner((event, timestamp) -> event.event_time)
                .withIdleness(Duration.ofSeconds(1));

        DataStream<SmartHomeEvent> events = env.fromSource(source, adwinStrategy, "Kafka Ingest");

        // Procesarea ferestrelor (10 secunde)
        DataStream<SmartHomeResult> results = events
                .keyBy(event -> event.house_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<SmartHomeEvent, SmartHomeResult, String, TimeWindow>() {
                    @Override
                    public void process(String houseId, Context context, Iterable<SmartHomeEvent> elements, Collector<SmartHomeResult> out) {
                        double sumTemp = 0;
                        double sumEnergy = 0;
                        long maxArrivalTime = 0;
                        int count = 0;

                        for (SmartHomeEvent event : elements) {
                            if (event.temperature != null) sumTemp += event.temperature;
                            if (event.energy_usage != null) sumEnergy += event.energy_usage;
                            maxArrivalTime = Math.max(maxArrivalTime, event.arrival_time);
                            count++;
                        }

                        long windowEnd = context.window().getEnd();
                        long now = System.currentTimeMillis();
                        long decisionalLatency = now - windowEnd;
                        long trueLatency = now - maxArrivalTime;

                        out.collect(new SmartHomeResult(
                                houseId,
                                windowEnd,
                                decisionalLatency,
                                trueLatency,
                                count,
                                sumTemp/count,
                                sumEnergy/count,
                                true, 1));
                    }
                });


        results.addSink(new InfluxDBSink("adaptive_real_adwin"));


       // results.map(r -> "🎯 ADWIN REAL EMIS -> Casa: " + r.houseId + " | Pachete: " + r.count).print();

        System.out.println("🚀 Job ADWIN Matematic pornit! (Nivel de încredere statistical: 95%)");
        env.execute("Smart City - Official ADWIN Analysis");
    }
}