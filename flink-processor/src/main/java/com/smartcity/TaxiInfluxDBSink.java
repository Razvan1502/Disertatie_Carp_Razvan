package com.smartcity;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import java.util.concurrent.TimeUnit;

public class TaxiInfluxDBSink extends RichSinkFunction<TaxiResult> {
    private transient InfluxDB influxDB;
    private final String strategy;

    public TaxiInfluxDBSink(String strategy) {
        this.strategy = strategy;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
        influxDB.setDatabase("smartcity");
        influxDB.enableBatch(100, 200, TimeUnit.MILLISECONDS);
    }

    @Override
    public void invoke(TaxiResult value, Context context) {
        Point point = Point.measurement("taxi_metrics")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .tag("zone_id", value.zoneId)
                .tag("strategy", strategy)
                .addField("true_latency_ms", value.trueLatencyMs)
                .addField("window_delay_s", value.windowDelayMs / 1000.0)
                .addField("event_count", (long) value.count)
                .addField("avg_distance", value.avgDistance)
                .addField("avg_duration", value.avgDuration)
                .addField("update_count", (long) value.updateCount)
                .addField("window_end", value.windowEnd)
                .build();
        influxDB.write(point);
    }

    @Override
    public void close() {
        if (influxDB != null) influxDB.close();
    }
}
