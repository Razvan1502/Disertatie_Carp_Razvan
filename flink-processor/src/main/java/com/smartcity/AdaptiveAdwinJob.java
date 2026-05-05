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
import java.util.LinkedList;

public class AdaptiveAdwinJob {

    // --- LOGICA ADWIN LITE PENTRU DETECȚIA SCHIMBĂRILOR STATISTICE ---
    public static class AdwinDetector {
        private final LinkedList<Long> samples = new LinkedList<>();
        private final int windowSize = 200; // Analizăm ultimele 200 de pachete

        public void addSample(long lateness) {
            samples.add(lateness);
            if (samples.size() > windowSize) samples.removeFirst();
        }

        public boolean hasDrift() {
            if (samples.size() < windowSize) return false;

            // Împărțim fereastra în două jumătăți (vechi vs nou)
            long sum1 = 0, sum2 = 0;
            int half = windowSize / 2;
            for (int i = 0; i < half; i++) sum1 += samples.get(i);
            for (int i = half; i < windowSize; i++) sum2 += samples.get(i);

            double avg1 = (double) sum1 / half;
            double avg2 = (double) sum2 / half;

            // Dacă media s-a schimbat cu mai mult de 1 secundă între cele două jumătăți
            return Math.abs(avg1 - avg2) > 1000;
        }

        public long getCurrentAvg() {
            if (samples.isEmpty()) return 0;
            long sum = 0;
            for (long v : samples) sum += v;
            return sum / samples.size();
        }
    }

    public static class AdwinWatermarkGenerator implements WatermarkGenerator<SmartHomeEvent> {
        private long maxTimestampSeen = Long.MIN_VALUE;
        private long lastEmittedWatermark = Long.MIN_VALUE;
        private long m = 5000; // Pornim de la 5s
        private final AdwinDetector adwin = new AdwinDetector();

        @Override
        public void onEvent(SmartHomeEvent event, long eventTimestamp, WatermarkOutput output) {
            maxTimestampSeen = Math.max(maxTimestampSeen, eventTimestamp);

            // Monitorizăm latența reală prin ADWIN
            long lateness = event.arrival_time - event.event_time;
            adwin.addSample(lateness);

            // Verificăm dacă ADWIN detectează un drift statistic
            if (adwin.hasDrift()) {
                long newAvg = adwin.getCurrentAvg();
                long oldM = m;

                // Adaptăm Slack-ul (m) cu un buffer de siguranță de 500ms
                m = Math.min(12000, newAvg + 500);

                if (m != oldM) {
                    System.out.printf("🚀 ADWIN DRIFT: Ajustez Slack de la %dms la %dms\n", oldM, m);
                }
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            if (maxTimestampSeen != Long.MIN_VALUE) {
                long potentialWM = maxTimestampSeen - m; //se genereaza watermark-ul
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
                .setGroupId("adaptive-adwin-group-" + System.currentTimeMillis())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(SmartHomeEvent.class))
                .build();

        WatermarkStrategy<SmartHomeEvent> strategy = WatermarkStrategy
                .forGenerator((ctx) -> new AdwinWatermarkGenerator())
                .withTimestampAssigner((event, timestamp) -> event.event_time)
                .withIdleness(Duration.ofSeconds(1));

        DataStream<SmartHomeResult> results = env.fromSource(source, strategy, "Kafka Ingest")
                .keyBy(event -> event.house_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<SmartHomeEvent, SmartHomeResult, String, TimeWindow>() {
                    @Override
                    public void process(String id, Context ctx, Iterable<SmartHomeEvent> el, Collector<SmartHomeResult> out) {
                        double sT = 0, sE = 0; int c = 0; long maxArrival = 0;
                        for (SmartHomeEvent e : el) { sT += e.temperature; sE += e.energy_usage; maxArrival = Math.max(maxArrival, e.arrival_time); c++; }
                        long wE = ctx.window().getEnd();
                        long now = System.currentTimeMillis();
                        out.collect(new SmartHomeResult(id, wE, now - wE, now - maxArrival, c, sT/c, sE/c, true, 1));
                    }
                });

        // Folosim tag-ul "adaptive_adwin" pentru a-l vedea separat în Grafana
        results.addSink(new InfluxDBSink("adaptive_adwin"));

        System.out.println("🚀 Job ADWIN pornind...");
        env.execute("Adaptive ADWIN Watermarking");
    }
}