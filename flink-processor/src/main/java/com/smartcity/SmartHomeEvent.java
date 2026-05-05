package com.smartcity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

// Această adnotare ignoră orice alte câmpuri care ar putea veni din JSON
// și pe care nu le-am definit (ajută mult la debugging)
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

    // Flink are nevoie obligatoriu de un constructor gol
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