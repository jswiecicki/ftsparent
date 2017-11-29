package com.CSBFTS.EventGenerator;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.TimeZone;


public class TestRunner {
    public static void main(String[] args) throws Exception {
        EventGenerator eGen = new EventGenerator();
       // eGen.bulkAddAccounts(1000000);
        eGen.runUpdateTest(50000,200,"event_gen_output.txt");
        /*
        if (args.length == 0) {
            eGen.runAddTest(15, 5, "event_gen_output.txt"); // arg specifies how many times to send a message
        } else {
            eGen.runAddTest(Integer.parseInt(args[0]), 5, "event_gen_output.txt");
        }
        */
    }
}
