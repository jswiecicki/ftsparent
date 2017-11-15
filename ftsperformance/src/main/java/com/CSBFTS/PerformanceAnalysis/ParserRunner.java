package com.CSBFTS.PerformanceAnalysis;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;

public class ParserRunner {
    public static void main(String[] args) {
        ArrayList<String[]> slowLogData = ElasticLogParser.parse();

        ArrayList<String[]> eventLogData = EventLogParser.parseEventLog();

        Iterator iterator = eventLogData.iterator();
        //eventLogData.it

        int j = 0;
        int i =0;
        while(iterator.hasNext() && j < slowLogData.size()) {
            String[] slowLogArr = slowLogData.get(j);
            String[] eventArr = eventLogData.get(i);


            String eventLogUID = eventArr[0];
            String eventLogTimestamp = eventArr[1];
            String eventLogEventType = eventArr[2];


            if(slowLogArr[3].equals(eventLogUID)) {
                System.out.println("id: "+eventLogUID + " kafka timestamp: " + eventLogTimestamp +
                                    "elastic receive ts: " + slowLogArr[0] +" took: " + slowLogArr[1] + " elastic sent ts: " +slowLogArr[2]);

                j++;
                //continue;
            }
            else{
                continue;
            }
        }
    }
}
