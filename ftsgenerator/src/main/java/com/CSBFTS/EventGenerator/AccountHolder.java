package com.CSBFTS.EventGenerator;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import java.io.IOException;
//import java.util.Date;
//import java.util.List;

public class AccountHolder {
    private int uniqueId;
    private String fname, lname, letters = "abcdefghijklmnopqrstuvwxyz";
    private double accountBalance;
    private long timeGenerated;


    public AccountHolder() {
        uniqueId = (int) (Math.random() * 1000000000); // 1 billion possibilities lowers the possibility of choosing the same id twice
        fname = "";
        lname = "";

        for (int i = 0; i < 5; i++) {
            fname += letters.charAt((int) (Math.random() * 26)); // create two distinct 5 letter names
            lname += letters.charAt((int) (Math.random() * 26));
        }

        accountBalance = Math.random() * 1000000; // starting balance is [0.0 to 1mil)
        timeGenerated = 0;
    }

    public void setTime(long currentTime) {
        timeGenerated = currentTime;
    }

    public String toJson() throws IOException {

        XContentBuilder xb =  XContentFactory.jsonBuilder()
                .startObject()
                .field("uniqueId", uniqueId)
                .field("fname", fname)
                .field("lname", lname)
                .field("accountBalance", accountBalance)
                .field("timeGenerated", timeGenerated)
                .endObject();

        return xb.string();
    }
}