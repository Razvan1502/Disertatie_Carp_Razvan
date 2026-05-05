package com.smartcity;

public class SmartHomeResult {
    public String houseId;
    public long windowEnd;
    public long decisionalLatencyMs; // System.currentTimeMillis() - window.getEnd(); poate fi negativ pt speculative
    public long trueLatencyMs;       // System.currentTimeMillis() - max(arrival_time); mereu >= 0
    public int count;
    public double avgTemp;
    public double avgEnergy;
    public boolean isFinal;
    public int updateCount;

    public SmartHomeResult() {}

    public SmartHomeResult(String houseId, long windowEnd, long decisionalLatencyMs, long trueLatencyMs,
                           int count, double avgTemp, double avgEnergy, boolean isFinal, int updateCount) {
        this.houseId = houseId;
        this.windowEnd = windowEnd;
        this.decisionalLatencyMs = decisionalLatencyMs;
        this.trueLatencyMs = trueLatencyMs;
        this.count = count;
        this.avgTemp = avgTemp;
        this.avgEnergy = avgEnergy;
        this.isFinal = isFinal;
        this.updateCount = updateCount;
    }
}