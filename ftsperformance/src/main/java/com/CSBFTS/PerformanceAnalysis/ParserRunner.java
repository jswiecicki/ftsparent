package com.CSBFTS.PerformanceAnalysis;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;

public class ParserRunner {
    public static void main(String[] args) {
        ArrayList<String[]> slowLogData = ElasticLogParser.parse();
        ArrayList<String[]> eventLogData = EventLogParser.parseEventLog();

        //int j = 0;
        //int i = 0;
        int counter=0;
        System.out.println("event log size: " + eventLogData.size() + " slow log size: "+slowLogData.size());
        for(int i =0; i < eventLogData.size(); i++) {
            for(int j = 0; j < slowLogData.size(); j++) {
                String[] eventLogArr = eventLogData.get(i);
                String[] slowLogArr = slowLogData.get(j);

                String eventLogUID = eventLogArr[0];
                String eventLogTimestamp = eventLogArr[1];
                String eventLogEventType = eventLogArr[2];

                //match
                if(eventLogUID.equals(slowLogArr[0])) {
                    //++counter++;
                    System.out.println(++counter+ " id: " + eventLogUID + "\tkafka timestamp: " + eventLogTimestamp +
                            " \telastic receive ts: " + slowLogArr[1] + "\ttook: " + slowLogArr[2] +
                            "\telastic sent ts: " + slowLogArr[3] + "\tevent type: " + eventLogEventType);

                }
            }
        }
        System.out.println("Done matching logs... ");
    }
}
