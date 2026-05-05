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
        // Configurarea mediului de execuție Flink
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //procesam secvential

        KafkaSource<SmartHomeEvent> source = KafkaSource.<SmartHomeEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("iot-sensor-data")
                .setGroupId("flink-processor-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(SmartHomeEvent.class)) //transf in json in ob smart event
                .build();

        WatermarkStrategy<SmartHomeEvent> watermarkStrategy = WatermarkStrategy
                .<SmartHomeEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> event.event_time); // flink sa ignore timmpul real si sa foloseasca event_time

        DataStream<SmartHomeEvent> events = env.fromSource(source, watermarkStrategy, "Kafka Ingest");

        //procesarea ferestrelor
        DataStream<SmartHomeResult> processedStream = events
                .keyBy(event -> event.house_id)// Grupăm datele pe fiecare casă
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

        processedStream.addSink(new InfluxDBSink("watermark"));
        env.execute("Smart City - Watermark Baseline");
    }
}