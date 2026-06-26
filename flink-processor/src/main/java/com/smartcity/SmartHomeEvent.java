package com.smartcity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


@JsonIgnoreProperties(ignoreUnknown = true)
public class SmartHomeEvent {

    @JsonProperty("house_id")
    public String house_id;

    @JsonProperty("temperature")
    public Double temperature;

    @JsonProperty("energy_usage")
    public Double energy_usage;

    @JsonProperty("humidity")
    public Double humidity;

    @JsonProperty("event_time")
    public Long event_time;

    @JsonProperty("arrival_time")
    public Long arrival_time;


    public SmartHomeEvent() {}

    @Override
    public String toString() {
        return "SmartHomeEvent{" +
                "house_id='" + house_id + '\'' +
                ", temperature=" + temperature +
                ", energy_usage=" + energy_usage +
                ", humidity=" + humidity +
                ", event_time=" + event_time +
                ", arrival_time=" + arrival_time +
                '}';
    }
}