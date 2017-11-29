package com.CSBFTS.EventGenerator;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
//import java.util.Date;
//import java.util.List;

public class AccountHolder {
    private int uniqueId;
    private String fname, lname, letters = "abcdefghijklmnopqrstuvwxyz";
    private double accountBalance;
    private String timeEventSent;
    private String eventType;


    public AccountHolder(String eventType){
        this.uniqueId = (int) (Math.random() * 1000000000); // 1 billion possibilities lowers the possibility of choosing the same id twice
        this.fname = "";
        this.lname = "";

        for (int i = 0; i < 5; i++) {
            this.fname += letters.charAt((int) (Math.random() * 26)); // create two distinct 5 letter names
            this.lname += letters.charAt((int) (Math.random() * 26));
        }

        this.accountBalance = Math.random() * 1000000; // starting balance is [0.0 to 1mil)
        this.timeEventSent = "";
        this.eventType = eventType;
    }

    public void setTime(String timeEventSent) {
        this.timeEventSent = timeEventSent;
    }

    public String toJson() throws IOException {

        XContentBuilder xb =  XContentFactory.jsonBuilder()
                .startObject()
                .field("uniqueId", uniqueId)
                .field("fname", fname)
                .field("lname", lname)
                .field("accountBalance", accountBalance)
                .field("timeEventSent", timeEventSent)
                .field("eventType", eventType)
                .endObject();

        return xb.string();
    }
}