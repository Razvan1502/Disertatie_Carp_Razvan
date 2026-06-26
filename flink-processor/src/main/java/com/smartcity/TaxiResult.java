package com.smartcity;

public class TaxiResult {
    public String zoneId;
    public long windowEnd;
    public long trueLatencyMs;
    public long windowDelayMs;
    public int count;
    public double avgDistance;       // mapped from temperature
    public double avgDuration;       // mapped from energy_usage
    public int updateCount;

    public TaxiResult() {}

    public TaxiResult(String zoneId, long windowEnd, long trueLatencyMs, long windowDelayMs,
                      int count, double avgDistance, double avgDuration, int updateCount) {
        this.zoneId = zoneId;
        this.windowEnd = windowEnd;
        this.trueLatencyMs = trueLatencyMs;
        this.windowDelayMs = windowDelayMs;
        this.count = count;
        this.avgDistance = avgDistance;
        this.avgDuration = avgDuration;
        this.updateCount = updateCount;
    }
}
