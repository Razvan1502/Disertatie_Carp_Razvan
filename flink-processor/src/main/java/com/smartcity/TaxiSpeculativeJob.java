package com.smartcity;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

public class TaxiSpeculativeJob {
    public static void main(String[] args) throws Exception {
        //pass mode as first argument (1, 2, or 3). Default: 3.
        // Mode 3: allowedLateness=3600s (covers max trip duration ~63min)
        // Mode 1/2: allowedLateness=120s (covers simulated OOO ~36s P95 + buffer)
        int mode = (args.length > 0) ? Integer.parseInt(args[0]) : 3;
        long allowedLatenessSeconds = (mode == 3) ? 3600 : 120;
        String sinkName = "taxi_speculative_m" + mode;

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String uniqueGroupId = "taxi-speculative-group-" + System.currentTimeMillis();
        KafkaSource<SmartHomeEvent> source = KafkaSource.<SmartHomeEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("iot-sensor-data")
                .setGroupId(uniqueGroupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(SmartHomeEvent.class))
                .build();

        // Zero slack
        WatermarkStrategy<SmartHomeEvent> watermarkStrategy = WatermarkStrategy
                .<SmartHomeEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((event, timestamp) -> event.event_time)
                .withIdleness(Duration.ofSeconds(5));

        DataStream<SmartHomeEvent> events = env.fromSource(source, watermarkStrategy, "Kafka Ingest");

        final long allowedLatenessSecondsFinal = allowedLatenessSeconds;
        events
                .keyBy(event -> event.house_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .allowedLateness(Time.seconds(allowedLatenessSecondsFinal))
                .trigger(org.apache.flink.streaming.api.windowing.triggers.CountTrigger.of(1))
                .process(new ProcessWindowFunction<SmartHomeEvent, TaxiResult, String, TimeWindow>() {
                    @Override
                    public void process(String zoneId, Context context, Iterable<SmartHomeEvent> elements, Collector<TaxiResult> out) throws Exception {
                        double sumDistance = 0, sumDuration = 0;
                        long maxArrivalTime = 0;
                        int count = 0;

                        for (SmartHomeEvent e : elements) {
                            if (e.temperature != null) sumDistance += e.temperature;
                            if (e.energy_usage != null) sumDuration += e.energy_usage;
                            maxArrivalTime = Math.max(maxArrivalTime, e.arrival_time);
                            count++;
                        }

                        ValueStateDescriptor<Long> trueLatDesc = new ValueStateDescriptor<>("taxiFirstTrueLat", Long.class);
                        ValueState<Long> firstTrueLatState = context.windowState().getState(trueLatDesc);
                        if (firstTrueLatState.value() == null) {
                            firstTrueLatState.update(System.currentTimeMillis() - maxArrivalTime);
                        }
                        long trueLatencyToReport = firstTrueLatState.value();

                        ValueStateDescriptor<Integer> countDesc = new ValueStateDescriptor<>("taxiLastCount", Integer.class);
                        ValueState<Integer> lastCountState = context.windowState().getState(countDesc);

                        if (lastCountState.value() == null || count > lastCountState.value()) {
                            ValueStateDescriptor<Integer> updateDesc = new ValueStateDescriptor<>("taxiUpdateCounter", Integer.class);
                            ValueState<Integer> updateCounterState = context.windowState().getState(updateDesc);
                            int currentUpdates = (updateCounterState.value() == null) ? 1 : updateCounterState.value() + 1;
                            updateCounterState.update(currentUpdates);

                            String logPrefix = currentUpdates == 1 ? "[INITIAL GUESS]" : "[SPECULATIVE CORRECTION]";
                            System.out.printf("%s Zone: %s | TrueLat: %dms | Update: %d | Events: %d%n",
                                    logPrefix, zoneId, trueLatencyToReport, currentUpdates, count);

                            long wm = context.currentWatermark();
                            long windowDelay = (wm == Long.MAX_VALUE) ? 0L : Math.max(0, wm - context.window().getEnd());

                            out.collect(new TaxiResult(
                                    zoneId,
                                    context.window().getEnd(),
                                    trueLatencyToReport,
                                    windowDelay,
                                    count,
                                    count > 0 ? sumDistance / count : 0,
                                    count > 0 ? sumDuration / count : 0,
                                    currentUpdates));

                            lastCountState.update(count);
                        }
                    }
                })
                .addSink(new TaxiInfluxDBSink(sinkName));

        System.out.printf("🚀 Taxi Speculative | Mode %d | AllowedLateness: %ds | Sink: %s%n",
                mode, allowedLatenessSeconds, sinkName);
        env.execute("Taxi - Speculative Mode " + mode);
    }
}
