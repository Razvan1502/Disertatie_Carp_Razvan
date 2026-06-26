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

public class TaxiAdaptiveAdwinJob {

    public static class TaxiAdwinDetector {
        private final LinkedList<Long> samples = new LinkedList<>();
        private final int windowSize = 200;
        private final long driftThresholdMs;

        public TaxiAdwinDetector(long driftThresholdMs) {
            this.driftThresholdMs = driftThresholdMs;
        }

        public void addSample(long oooLateness) {
            samples.add(oooLateness);
            if (samples.size() > windowSize) samples.removeFirst();
        }

        public boolean hasDrift() {
            if (samples.size() < windowSize) return false;
            long sum1 = 0, sum2 = 0;
            int half = windowSize / 2;
            for (int i = 0; i < half; i++) sum1 += samples.get(i);
            for (int i = half; i < windowSize; i++) sum2 += samples.get(i);
            double avg1 = (double) sum1 / half;
            double avg2 = (double) sum2 / half;
            return Math.abs(avg1 - avg2) > driftThresholdMs;
        }

        public long getCurrentAvg() {
            if (samples.isEmpty()) return 0;
            long sum = 0;
            for (long v : samples) sum += v;
            return sum / samples.size();
        }
    }

    public static class TaxiAdwinWatermarkGenerator implements WatermarkGenerator<SmartHomeEvent> {
        private long maxTimestampSeen = Long.MIN_VALUE;
        private long lastEmittedWatermark = Long.MIN_VALUE;
        private long m;
        private final long maxSlackMs;
        private final long minSlackMs;
        private final long bufferMs;
        private final TaxiAdwinDetector adwin;

        public TaxiAdwinWatermarkGenerator(int mode, long driftOverrideMs) {
            if (mode == 3) {
                m          = 600_000;
                maxSlackMs = 3_600_000;
                minSlackMs = 10_000;
                bufferMs   = 60_000;
                adwin      = new TaxiAdwinDetector(driftOverrideMs > 0 ? driftOverrideMs : 60_000);
            } else {
                m          = 36_000;
                maxSlackMs = 250_000;
                minSlackMs = 1_000;
                bufferMs   = 5_000;
                adwin      = new TaxiAdwinDetector(driftOverrideMs > 0 ? driftOverrideMs : 500);
            }
        }

        @Override
        public void onEvent(SmartHomeEvent event, long eventTimestamp, WatermarkOutput output) {
            long oldMax = maxTimestampSeen;
            maxTimestampSeen = Math.max(maxTimestampSeen, eventTimestamp);

            if (oldMax != Long.MIN_VALUE) {
                long oooLateness = Math.max(0, oldMax - eventTimestamp);
                adwin.addSample(oooLateness);

                if (adwin.hasDrift()) {
                    long newAvg = adwin.getCurrentAvg();
                    long oldM = m;
                    m = Math.min(maxSlackMs, Math.max(minSlackMs, newAvg + bufferMs));
                    if (m != oldM) {
                        System.out.printf("🚀 TAXI ADWIN DRIFT: Slack %ds → %ds (avg ooo=%.1fs)%n",
                                oldM / 1000, m / 1000, newAvg / 1000.0);
                    }
                }
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
        // Usage: pass mode as first argument (1, 2, or 3). Default: 3.
        // args[0] = mode (default 3), args[1] = driftThreshold in seconds (default: 60s for mode 3)
        int mode = (args.length > 0) ? Integer.parseInt(args[0]) : 3;
        long driftSec = (args.length > 1) ? Long.parseLong(args[1]) : -1;
        long driftMs = driftSec > 0 ? driftSec * 1000L : -1;
        String sinkName = (args.length > 1)
                ? String.format("taxi_adaptive_adwin_m%d_dt%d", mode, driftSec)
                : "taxi_adaptive_adwin_m" + mode;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String uniqueGroupId = "taxi-adaptive-adwin-group-" + System.currentTimeMillis();
        KafkaSource<SmartHomeEvent> source = KafkaSource.<SmartHomeEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("iot-sensor-data")
                .setGroupId(uniqueGroupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(SmartHomeEvent.class))
                .build();

        final int modeFinal = mode;
        final long driftMsFinal = driftMs;
        WatermarkStrategy<SmartHomeEvent> strategy = WatermarkStrategy
                .forGenerator((ctx) -> new TaxiAdwinWatermarkGenerator(modeFinal, driftMsFinal))
                .withTimestampAssigner((event, timestamp) -> event.event_time)
                .withIdleness(Duration.ofSeconds(5));

        DataStream<TaxiResult> results = env.fromSource(source, strategy, "Kafka Ingest")
                .keyBy(event -> event.house_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .process(new ProcessWindowFunction<SmartHomeEvent, TaxiResult, String, TimeWindow>() {
                    @Override
                    public void process(String zoneId, Context ctx,
                                        Iterable<SmartHomeEvent> elements, Collector<TaxiResult> out) {
                        double sumDist = 0, sumDur = 0;
                        long maxArrival = 0;
                        int count = 0;
                        for (SmartHomeEvent e : elements) {
                            if (e.temperature != null) sumDist += e.temperature;
                            if (e.energy_usage != null) sumDur  += e.energy_usage;
                            maxArrival = Math.max(maxArrival, e.arrival_time);
                            count++;
                        }
                        long wm = ctx.currentWatermark();
                        long windowDelay = (wm == Long.MAX_VALUE) ? 0L : Math.max(0, wm - ctx.window().getEnd());
                        out.collect(new TaxiResult(zoneId, ctx.window().getEnd(),
                                System.currentTimeMillis() - maxArrival,
                                windowDelay,
                                count,
                                count > 0 ? sumDist / count : 0,
                                count > 0 ? sumDur  / count : 0, 1));
                    }
                });

        results.addSink(new TaxiInfluxDBSink(sinkName));
        long effectiveDrift = driftMs > 0 ? driftMs : (mode == 3 ? 60_000 : 500);
        System.out.printf("🚀 Taxi Adaptive ADWIN | Mode %d | driftThreshold=%ds | Sink: %s%n",
                mode, effectiveDrift / 1000, sinkName);
        env.execute("Taxi - Adaptive ADWIN Mode " + mode);
    }
}
