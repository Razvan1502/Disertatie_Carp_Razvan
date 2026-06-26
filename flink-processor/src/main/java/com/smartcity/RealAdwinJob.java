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

        // Parametrii Algorithm 1
        private long lateElements = 0;
        private long totalElements = 0;
        private double delta = 1.0;
        private final double L;
        private final double deltaStep;

        public OfficialAdwinWatermarkGenerator(double L, double deltaStep) {
            this.L = L;
            this.deltaStep = deltaStep;
        }

        // Warmup
        private final int WARMUP_COUNT = 500;
        private int warmupDone = 0;
        private long warmupSum = 0;
        private long currentSlack = 2000; // valoare de start pana la finalizarea warmup-ului

        private final ADWIN adwin = new ADWIN(delta);

        @Override
        public void onEvent(SmartHomeEvent event, long eventTimestamp, WatermarkOutput output) {
            maxTimestampSeen = Math.max(maxTimestampSeen, eventTimestamp);

            long lateness = Math.max(0, event.arrival_time - event.event_time);

            // Faza warmup: m = medie(tp(e) - te(e)) + buffer
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

            // Normalizare input la [0,1] : (tp-te)/m
            double normalizedLateness = (currentSlack > 0) ? (double) lateness / currentSlack : 0.0;

            normalizedLateness = Math.min(normalizedLateness, 2.0);

            boolean isDriftDetected = adwin.setInput(normalizedLateness);

            if (isDriftDetected) {
                double currentLateRate = (totalElements > 0) ? (double) lateElements / totalElements : 0.0;

                // creștem sensibilitatea.
                if (lateElements == 0) {
                    delta = Math.min(1.0, delta + deltaStep);
                    adwin.setDelta(delta);
                }


                if (currentLateRate < L) {
                    lateElements = 0;
                    totalElements = 0;
                    System.out.printf("✅ ADWIN DRIFT: lateRate=%.2f < L=%.2f → m neschimbat (%dms), reset contoare, δ=%.2f\n",
                            currentLateRate, L, currentSlack, delta);
                } else {
                    //prea multi tardivi → updateSkewness() + scadem sensibilitatea.

                    long newSlack = (long)(adwin.getEstimation() * currentSlack) + 1000;
                    currentSlack = Math.min(12000, Math.max(1000, newSlack));
                    delta = Math.max(0.01, delta - deltaStep);
                    adwin.setDelta(delta);
                    System.out.printf("⚠️ ADWIN DRIFT: lateRate=%.2f >= L=%.2f → updateSkewness m=%dms, δ↓=%.2f\n",
                            currentLateRate, L, currentSlack, delta);
                }
            } else {

                if (lastEmittedWatermark != Long.MIN_VALUE && eventTimestamp < lastEmittedWatermark) {
                    lateElements++;
                }
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // Nu emitem watermark in faza de warmup
            if (warmupDone < WARMUP_COUNT || maxTimestampSeen == Long.MIN_VALUE) return;

            long potentialWM = maxTimestampSeen - currentSlack;
            if (potentialWM > lastEmittedWatermark) {
                lastEmittedWatermark = potentialWM;
                output.emitWatermark(new Watermark(lastEmittedWatermark));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        // arg[0] = L (late arrival threshold, default 0.1). Ex: 0.01, 0.1, 1.0
        // arg[1] = deltaStep (∆δ, default 0.1). Ex: 0.1, 1.0
        double L         = (args.length > 0) ? Double.parseDouble(args[0]) : 0.1;
        double deltaStep = (args.length > 1) ? Double.parseDouble(args[1]) : 0.1;
        String sinkName  = String.format("adaptive_real_adwin_L%d_dd%d",
                (int)(L * 100), (int)(deltaStep * 100));

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

        final double Lf = L, dsf = deltaStep;
        WatermarkStrategy<SmartHomeEvent> adwinStrategy = WatermarkStrategy
                .forGenerator((ctx) -> new OfficialAdwinWatermarkGenerator(Lf, dsf))
                .withTimestampAssigner((event, timestamp) -> event.event_time)
                .withIdleness(Duration.ofSeconds(1));

        DataStream<SmartHomeEvent> events = env.fromSource(source, adwinStrategy, "Kafka Ingest");


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


        results.addSink(new InfluxDBSink(sinkName));
        System.out.printf("🚀 Official ADWIN | L=%.2f | ∆δ=%.2f | Sink: %s%n", L, deltaStep, sinkName);
        env.execute("Smart Home - Official ADWIN L=" + L + " dd=" + deltaStep);
    }
}