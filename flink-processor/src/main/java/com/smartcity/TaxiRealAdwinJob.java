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

public class TaxiRealAdwinJob {

    public static class TaxiOfficialAdwinWatermarkGenerator implements WatermarkGenerator<SmartHomeEvent> {
        private long maxTimestampSeen = Long.MIN_VALUE;
        private long lastEmittedWatermark = Long.MIN_VALUE;

        private long lateElements = 0;
        private long totalElements = 0;
        private double delta = 1.0;
        private final double L;
        private final double deltaStep;

        private final int WARMUP_COUNT = 500;
        private int warmupDone = 0;
        private long warmupSum = 0;

        private long currentSlack;
        private final long maxSlack;
        private final long minSlack;
        private final long warmupBuffer;
        private final long driftBufferHigh;

        private final ADWIN adwin = new ADWIN(delta);

        public TaxiOfficialAdwinWatermarkGenerator(int mode, double L, double deltaStep) {
            this.L = L;
            this.deltaStep = deltaStep;
            if (mode == 3) {
                currentSlack  = 600_000;
                maxSlack      = 3_600_000;
                minSlack      = 10_000;
                warmupBuffer  = 60_000;
                driftBufferHigh = 120_000;
            } else {
                currentSlack  = 36_000;
                maxSlack      = 250_000;
                minSlack      = 1_000;
                warmupBuffer  = 5_000;
                driftBufferHigh = 10_000;
            }
        }

        @Override
        public void onEvent(SmartHomeEvent event, long eventTimestamp, WatermarkOutput output) {
            long oldMax = maxTimestampSeen;
            maxTimestampSeen = Math.max(maxTimestampSeen, eventTimestamp);

            long lateness = (oldMax != Long.MIN_VALUE) ? Math.max(0, oldMax - eventTimestamp) : 0;

            if (warmupDone < WARMUP_COUNT) {
                warmupSum += lateness;
                warmupDone++;
                if (warmupDone == WARMUP_COUNT) {
                    long avgLateness = warmupSum / WARMUP_COUNT;
                    currentSlack = Math.min(maxSlack, Math.max(minSlack, avgLateness + warmupBuffer));
                    System.out.printf("✅ TAXI ADWIN Warmup finalizat: avgOOO=%ds → slack=%ds\n",
                            avgLateness / 1000, currentSlack / 1000);
                }
                return;
            }

            totalElements++;

            double normalizedLateness = (currentSlack > 0) ? (double) lateness / currentSlack : 0.0;
            normalizedLateness = Math.min(normalizedLateness, 2.0);

            boolean isDriftDetected = adwin.setInput(normalizedLateness);

            if (isDriftDetected) {
                double currentLateRate = (totalElements > 0) ? (double) lateElements / totalElements : 0.0;

                // fara tardivi → crestem sensibilitatea.
                if (lateElements == 0) {
                    delta = Math.min(1.0, delta + deltaStep);
                    adwin.setDelta(delta);
                }


                if (currentLateRate < L) {
                    lateElements = 0;
                    totalElements = 0;
                    System.out.printf("✅ TAXI ADWIN DRIFT: lateRate=%.2f < L=%.2f → m neschimbat (%ds), reset contoare, δ=%.2f\n",
                            currentLateRate, L, currentSlack / 1000, delta);
                } else {
                    //prea multi tardivi → updateSkewness() + scadem sensibilitatea.

                    long newSlack = (long)(adwin.getEstimation() * currentSlack) + driftBufferHigh;
                    currentSlack = Math.min(maxSlack, Math.max(minSlack, newSlack));
                    delta = Math.max(0.01, delta - deltaStep);
                    adwin.setDelta(delta);
                    System.out.printf("⚠️ TAXI ADWIN DRIFT: lateRate=%.2f >= L=%.2f → updateSkewness slack=%ds, δ↓=%.2f\n",
                            currentLateRate, L, currentSlack / 1000, delta);
                }
            } else {

                if (lastEmittedWatermark != Long.MIN_VALUE && eventTimestamp < lastEmittedWatermark) {
                    lateElements++;
                }
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            if (warmupDone < WARMUP_COUNT || maxTimestampSeen == Long.MIN_VALUE) return;

            long potentialWM = maxTimestampSeen - currentSlack;
            if (potentialWM > lastEmittedWatermark) {
                lastEmittedWatermark = potentialWM;
                output.emitWatermark(new Watermark(lastEmittedWatermark));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // args[0] = mode (default 3), args[1] = L (default 0.1), args[2] = deltaStep (default 0.1)
        int mode = (args.length > 0) ? Integer.parseInt(args[0]) : 3;
        double L = (args.length > 1) ? Double.parseDouble(args[1]) : 0.1;
        double deltaStep = (args.length > 2) ? Double.parseDouble(args[2]) : 0.1;
        String sinkName = (args.length > 1)
                ? String.format("taxi_real_adwin_m%d_L%d_dd%d", mode, (int)(L*100), (int)(deltaStep*100))
                : "taxi_real_adwin_m" + mode;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String uniqueGroupId = "taxi-real-adwin-group-" + System.currentTimeMillis();
        KafkaSource<SmartHomeEvent> source = KafkaSource.<SmartHomeEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("iot-sensor-data")
                .setGroupId(uniqueGroupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(SmartHomeEvent.class))
                .build();

        final int modeFinal = mode;
        final double Lf = L, dsf = deltaStep;
        WatermarkStrategy<SmartHomeEvent> adwinStrategy = WatermarkStrategy
                .forGenerator((ctx) -> new TaxiOfficialAdwinWatermarkGenerator(modeFinal, Lf, dsf))
                .withTimestampAssigner((event, timestamp) -> event.event_time)
                .withIdleness(Duration.ofSeconds(5));

        DataStream<TaxiResult> results = env.fromSource(source, adwinStrategy, "Kafka Ingest")
                .keyBy(event -> event.house_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .process(new ProcessWindowFunction<SmartHomeEvent, TaxiResult, String, TimeWindow>() {
                    @Override
                    public void process(String zoneId, Context context, Iterable<SmartHomeEvent> elements, Collector<TaxiResult> out) {
                        double sumDistance = 0, sumDuration = 0;
                        long maxArrivalTime = 0;
                        int count = 0;

                        for (SmartHomeEvent e : elements) {
                            if (e.temperature != null) sumDistance += e.temperature;
                            if (e.energy_usage != null) sumDuration += e.energy_usage;
                            maxArrivalTime = Math.max(maxArrivalTime, e.arrival_time);
                            count++;
                        }

                        long wm = context.currentWatermark();
                        long windowDelay = (wm == Long.MAX_VALUE) ? 0L : Math.max(0, wm - context.window().getEnd());
                        out.collect(new TaxiResult(
                                zoneId,
                                context.window().getEnd(),
                                System.currentTimeMillis() - maxArrivalTime,
                                windowDelay,
                                count,
                                count > 0 ? sumDistance / count : 0,
                                count > 0 ? sumDuration / count : 0,
                                1));
                    }
                });

        results.addSink(new TaxiInfluxDBSink(sinkName));
        System.out.printf("🚀 Taxi Official ADWIN | Mode %d | L=%.2f | Δδ=%.2f | Sink: %s%n", mode, L, deltaStep, sinkName);
        env.execute("Taxi - Official ADWIN Mode " + mode);
    }
}
