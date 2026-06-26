package com.smartcity;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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


public class TaxiNaiveJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String uniqueGroupId = "taxi-naive-group-" + System.currentTimeMillis();
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

        DataStream<TaxiResult> results = events
                .keyBy(event -> event.house_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .process(new ProcessWindowFunction<SmartHomeEvent, TaxiResult, String, TimeWindow>() {
                    @Override
                    public void process(String zoneId, Context context,
                                        Iterable<SmartHomeEvent> elements, Collector<TaxiResult> out) {
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

        results.addSink(new TaxiInfluxDBSink("taxi_naive"));
        System.out.println("🚀 Taxi Naive Job pornit! (slack 0s, fără allowedLateness — baseline fără OOO handling)");
        env.execute("Taxi - Naive Baseline (No OOO Strategy)");
    }
}
