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

public class SpeculativeJob {
    public static void main(String[] args) throws Exception {
        // Configurarea mediului de execuție Flink
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Configurare sursa Kafka
        String uniqueGroupId = "speculative-group-" + System.currentTimeMillis();
        KafkaSource<SmartHomeEvent> source = KafkaSource.<SmartHomeEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("iot-sensor-data")
                .setGroupId(uniqueGroupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(SmartHomeEvent.class))
                .build();


        WatermarkStrategy<SmartHomeEvent> watermarkStrategy = WatermarkStrategy
                .<SmartHomeEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((event, timestamp) -> event.event_time)
                .withIdleness(Duration.ofSeconds(1));

        DataStream<SmartHomeEvent> events = env.fromSource(source, watermarkStrategy, "Kafka Ingest");


        events
                .keyBy(event -> event.house_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(15)) //pentru corectii
                // CountTrigger pentru reacție instantanee la pachete
                .trigger(org.apache.flink.streaming.api.windowing.triggers.CountTrigger.of(1))
                .process(new ProcessWindowFunction<SmartHomeEvent, SmartHomeResult, String, TimeWindow>() {
                    @Override
                    public void process(String houseId, Context context, Iterable<SmartHomeEvent> elements, Collector<SmartHomeResult> out) throws Exception {

                        double sumTemp = 0;
                        double sumEnergy = 0;
                        long maxArrivalTime = 0;
                        int count = 0;

                        for (SmartHomeEvent e : elements) {
                            if (e.temperature != null) sumTemp += e.temperature;
                            if (e.energy_usage != null) sumEnergy += e.energy_usage;
                            maxArrivalTime = Math.max(maxArrivalTime, e.arrival_time);
                            count++;
                        }

                        double avgTemp = count > 0 ? sumTemp / count : 0;
                        double avgEnergy = count > 0 ? sumEnergy / count : 0;

                        long windowEnd = context.window().getEnd();
                        long systemTime = System.currentTimeMillis();

                        // Decisional latency: salvam prima valoare
                        ValueStateDescriptor<Long> latDesc = new ValueStateDescriptor<>("firstDecisionalLat", Long.class);
                        ValueState<Long> firstLatState = context.windowState().getState(latDesc);
                        if (firstLatState.value() == null) {
                            firstLatState.update(systemTime - windowEnd);
                        }
                        long decisionalLatencyToReport = firstLatState.value();

                        // True latency: salvam prima valoare
                        ValueStateDescriptor<Long> trueLatDesc = new ValueStateDescriptor<>("firstTrueLat", Long.class);
                        ValueState<Long> firstTrueLatState = context.windowState().getState(trueLatDesc);
                        if (firstTrueLatState.value() == null) {
                            firstTrueLatState.update(systemTime - maxArrivalTime);
                        }
                        long trueLatencyToReport = firstTrueLatState.value();

                        ValueStateDescriptor<Integer> countDesc = new ValueStateDescriptor<>("lastCount", Integer.class);
                        ValueState<Integer> lastCountState = context.windowState().getState(countDesc);

                        if (lastCountState.value() == null || count > lastCountState.value()) {

                            ValueStateDescriptor<Integer> updateDesc = new ValueStateDescriptor<>("updateCounter", Integer.class);
                            ValueState<Integer> updateCounterState = context.windowState().getState(updateDesc);
                            int currentUpdates = (updateCounterState.value() == null) ? 1 : updateCounterState.value() + 1;
                            updateCounterState.update(currentUpdates);

                            boolean isFinal = (systemTime - windowEnd) > 15000;

                            String logPrefix = isFinal ? "[FINAL UPDATE]" : (currentUpdates == 1 ? "[INITIAL GUESS]" : "[SPECULATIVE CORRECTION]");
                            System.out.println(String.format("%s Casa: %s | DecisionalLat: %dms | TrueLat: %dms | Update: %d | Pachete: %d",
                                    logPrefix, houseId, decisionalLatencyToReport, trueLatencyToReport, currentUpdates, count));

                            if (currentUpdates == 1) {
                                out.collect(new SmartHomeResult(
                                        houseId + "-initial",
                                        windowEnd,
                                        decisionalLatencyToReport,
                                        trueLatencyToReport,
                                        count,
                                        avgTemp,
                                        avgEnergy,
                                        false,
                                        1
                                ));
                            }

                            out.collect(new SmartHomeResult(
                                    houseId, windowEnd, decisionalLatencyToReport, trueLatencyToReport, count, avgTemp, avgEnergy, isFinal, currentUpdates
                            ));

                            lastCountState.update(count);
                        }
                    }
                })
                .addSink(new InfluxDBSink("speculative"));

        env.execute("Speculative Job - Analysis");
    }
}