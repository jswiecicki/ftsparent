package com.CSBFTS.EventGenerator;

import java.util.Date;

public class Event {
    private String event; // what the specified event is
    private Date timeGenerated; // time (in milliseconds) the event was generated

    public Event() {
        // eventually make this a randomly generated event
        this.event = "TEST";
        this.timeGenerated = new Date();
    }

    public String getEvent() {
        return event;
    }

    public Date getTimeGenerated() {
        return timeGenerated;
    }

    @Override
    public String toString() {
        return "{" +
                "\"event\": " + "\"" + getEvent() + "\"," +
                "\"timestamp\": " + "\"" + getTimeGenerated().getTime() + "\"" +
                "}";
    }

}