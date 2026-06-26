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

public class DataStreamJob {
    public static void main(String[] args) throws Exception {
        // arg[0] = slackSeconds (default 5). Ex: 1, 3, 5, 8, 12
        long slackSeconds = (args.length > 0) ? Long.parseLong(args[0]) : 5;
        String sinkName = "watermark_" + slackSeconds + "s";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<SmartHomeEvent> source = KafkaSource.<SmartHomeEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("iot-sensor-data")
                .setGroupId("watermark-group-" + System.currentTimeMillis())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(SmartHomeEvent.class))
                .build();

        WatermarkStrategy<SmartHomeEvent> watermarkStrategy = WatermarkStrategy
                .<SmartHomeEvent>forBoundedOutOfOrderness(Duration.ofSeconds(slackSeconds))
                .withTimestampAssigner((event, timestamp) -> event.event_time)
                .withIdleness(Duration.ofSeconds(1));

        DataStream<SmartHomeEvent> events = env.fromSource(source, watermarkStrategy, "Kafka Ingest");

        //procesarea ferestrelor
        DataStream<SmartHomeResult> processedStream = events
                .keyBy(event -> event.house_id)// Grupam datele pe fiecare casa
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<SmartHomeEvent, SmartHomeResult, String, TimeWindow>() {
                    @Override
                    public void process(String houseId, Context context, Iterable<SmartHomeEvent> elements, Collector<SmartHomeResult> out) {
                        double sumTemp = 0, sumEnergy = 0;
                        long maxArrivalTime = 0;
                        int count = 0;

                        for (SmartHomeEvent event : elements) {
                            if (event.temperature != null) sumTemp += event.temperature;
                            if (event.energy_usage != null) sumEnergy += event.energy_usage;
                            maxArrivalTime = Math.max(maxArrivalTime, event.arrival_time);
                            count++;
                        }

                        double avgTemp = count > 0 ? sumTemp / count : 0;
                        double avgEnergy = count > 0 ? sumEnergy / count : 0;

                        long windowEndMs = context.window().getEnd();
                        long now = System.currentTimeMillis();
                        long decisionalLatencyMs = now - windowEndMs;
                        long trueLatencyMs = now - maxArrivalTime;

                        out.collect(new SmartHomeResult(houseId, windowEndMs, decisionalLatencyMs, trueLatencyMs, count, avgTemp, avgEnergy, true, 1));
                    }
                });

        processedStream.addSink(new InfluxDBSink(sinkName));
        System.out.printf("🚀 Watermark Fix | slack=%ds | Sink: %s%n", slackSeconds, sinkName);
        env.execute("Smart Home - Watermark Baseline slack=" + slackSeconds + "s");
    }
}