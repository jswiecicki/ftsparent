package com.CSBFTS.PerformanceAnalysis;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;

public class ParserRunner {
    public static void main(String[] args) {
        ArrayList<String[]> slowLogData = ElasticLogParser.parse();

        HashMap<String,Long> eventLogMap = EventLogParser.parseEventLog();

        Iterator i = eventLogMap.entrySet().iterator();

        int j = 0;
        while(i.hasNext() && j < slowLogData.size()) {
            String[] arr = slowLogData.get(j);

            HashMap.Entry pair = (HashMap.Entry)i.next();
            Timestamp t = new Timestamp((Long)pair.getValue());

            String eventLogUID = (String)pair.getKey();
            String eventLogTimestamp = t.toString();


            if(arr[3].equals(eventLogUID)) {
                System.out.println("id: "+eventLogUID + " kafka timestamp: " + eventLogTimestamp +
                                    "elastic recieve ts: " + arr[0] +" took: " + arr[1] + " elastic sent ts: " +arr[2]);

                j++;
            }
            else{
                continue;
            }
        }
    }
}
