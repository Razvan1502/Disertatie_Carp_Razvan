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

public class NaiveJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String uniqueGroupId = "naive-group-" + System.currentTimeMillis();
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
                .withIdleness(Duration.ofSeconds(1));

        DataStream<SmartHomeEvent> events = env.fromSource(source, watermarkStrategy, "Kafka Ingest");

        DataStream<SmartHomeResult> results = events
                .keyBy(event -> event.house_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<SmartHomeEvent, SmartHomeResult, String, TimeWindow>() {
                    @Override
                    public void process(String houseId, Context context,
                                        Iterable<SmartHomeEvent> elements, Collector<SmartHomeResult> out) {
                        double sumTemp = 0, sumEnergy = 0;
                        long maxArrivalTime = 0;
                        int count = 0;

                        for (SmartHomeEvent e : elements) {
                            if (e.temperature != null) sumTemp += e.temperature;
                            if (e.energy_usage != null) sumEnergy += e.energy_usage;
                            maxArrivalTime = Math.max(maxArrivalTime, e.arrival_time);
                            count++;
                        }

                        long windowEnd = context.window().getEnd();
                        long now = System.currentTimeMillis();
                        out.collect(new SmartHomeResult(
                                houseId, windowEnd,
                                now - windowEnd,
                                now - maxArrivalTime,
                                count,
                                count > 0 ? sumTemp / count : 0,
                                count > 0 ? sumEnergy / count : 0,
                                true, 1));
                    }
                });

        results.addSink(new InfluxDBSink("naive"));
        System.out.println("🚀 Naive Smart Home Job pornit! (slack=0s — fără OOO handling, baseline pur)");
        env.execute("Smart Home - Naive Baseline");
    }
}
