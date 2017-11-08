package com.CSBFTS.PerformanceAnalysis;

import java.util.HashMap;
import java.util.Iterator;

public class ParserRunner {
    public static void main(String[] args) {
        HashMap<String,Long> map = EventLogParser.parseEventLog();

        Iterator i = map.entrySet().iterator();
        while(i.hasNext()) {
            HashMap.Entry pair = (HashMap.Entry)i.next();
            System.out.println("id: "+pair.getKey() + " timestamp: " + pair.getValue());
        }

    }
}
