package com.CSBFTS.PerformanceAnalysis;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;

public class ParserRunner {
    public static void main(String[] args) {
        ArrayList<String[]> slowLogData = ElasticLogParser.parse();
        ArrayList<String[]> eventLogData = EventLogParser.parseEventLog();

        int j = 0;
        int i = 0;
        System.out.println("event log size: " + eventLogData.size() + " slow log size: "+slowLogData.size());
        while(i < eventLogData.size() && j < slowLogData.size() && j <slowLogData.size()) {
            //while(j < slowLogData.size()) {
                String[] eventLogArr = eventLogData.get(i);
                String[] slowLogArr = slowLogData.get(j);

                String eventLogUID = eventLogArr[0];
                String eventLogTimestamp = eventLogArr[1];
                String eventLogEventType = eventLogArr[2];

/*
                System.out.println("id: " + eventLogUID + " kafka timestamp: " + eventLogTimestamp +
                    "elastic receive ts: " + slowLogArr[1] + " took: " + slowLogArr[2] +
                    " elastic sent ts: " + slowLogArr[3] + " event type: " + eventLogEventType);
*/
                //match
                if(eventLogUID.equals(slowLogArr[0])) {
                    System.out.println("id: " + eventLogUID + "\tkafka timestamp: " + eventLogTimestamp +
                            " \telastic receive ts: " + slowLogArr[1] + "\ttook: " + slowLogArr[2] +
                            "\telastic sent ts: " + slowLogArr[3] + "\tevent type: " + eventLogEventType);

                    //move to next event log line
                    i++;
                }
                //no match
                else {
                    //move to next slow log line
                    j++;

                }
            //}
        }
        System.out.println("Done matching logs... If nothing printed out then nothing matched up. Sorry");
    }
}
