package com.smartcity;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import java.util.concurrent.TimeUnit;

public class InfluxDBSink extends RichSinkFunction<SmartHomeResult> {
    private transient InfluxDB influxDB;
    private final String strategy;

    public InfluxDBSink(String strategy) {
        this.strategy = strategy;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
        influxDB.setDatabase("smartcity");
        influxDB.enableBatch(100, 200, TimeUnit.MILLISECONDS);
    }

    @Override
    public void invoke(SmartHomeResult value, Context context) {
        Point point = Point.measurement("house_metrics")
                .time(value.windowEnd, TimeUnit.MILLISECONDS) //timpul este windowend, ajuta la mecanismul de retracii/ update in db pt corectii
                .tag("house_id", value.houseId)
                .tag("strategy", strategy)
                .addField("avg_temp", value.avgTemp)
                .addField("avg_energy", value.avgEnergy)
                .addField("decisional_latency_ms", value.decisionalLatencyMs)
                .addField("true_latency_ms", value.trueLatencyMs)
                .addField("pachete_count", value.count)
                .addField("is_final", value.isFinal)
                .addField("update_count", value.updateCount)
                .build();
        influxDB.write(point);
    }

    @Override
    public void close() {
        if (influxDB != null) influxDB.close();
    }
}