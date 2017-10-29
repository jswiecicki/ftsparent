package com.CSBFTS.EventGenerator;

import com.fasterxml.jackson.databind.ObjectMapper;



public class TestRunner {
    public static void main(String[] args) throws Exception {
        EventGenerator eGen = new EventGenerator();

        if (args.length == 0) {

            eGen.runAddTest(10); // arg specifies how many times to send a message
        } else {
            eGen.runAddTest(Integer.parseInt(args[0]));
        }
    }
}
