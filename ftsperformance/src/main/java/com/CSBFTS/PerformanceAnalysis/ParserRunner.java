package com.CSBFTS.PerformanceAnalysis;

import java.sql.Timestamp;
import java.util.ArrayList;

public class ParserRunner {
    public static void main(String[] args) {
        ArrayList<String[]> slowLogData = ElasticLogParser.parse();
        ArrayList<String[]> eventLogData = EventLogParser.parseEventLog();

        //int j = 0;
        //int i = 0;
        int counter=1;
        System.out.println("event log size: " + eventLogData.size() + " slow log size: "+slowLogData.size());
        for(int i =0; i < eventLogData.size(); i++) {
            for(int j = 0; j < slowLogData.size(); j++) {
                String[] eventLogArr = eventLogData.get(i);
                String[] slowLogArr = slowLogData.get(j);

                String eventLogUID = eventLogArr[0];
                String eventLogTimestamp = eventLogArr[1];
                String eventLogEventType = eventLogArr[2];

                String slowLogReceiveTimestamp = slowLogArr[1];
                String slowLogTook = slowLogArr[2];
                String slowLogSentTimestamp = slowLogArr[3];

                //match
                if(eventLogUID.equals(slowLogArr[0])) {


                    System.out.print(counter+ " event UID: " + eventLogUID + "\tkafka timestamp: " + eventLogTimestamp +
                            " \telastic receive ts: " + slowLogReceiveTimestamp + "\ttook: " + slowLogTook +
                            "\telastic sent ts: " + slowLogSentTimestamp + "\tevent type: " + eventLogEventType);
                    counter++;

                    //Timestamp t1 = new Timestamp(String(slowLogArr[1]));
                    Timestamp kafka_ts = Timestamp.valueOf(eventLogTimestamp);
                    //Timestamp k_ts = new Timestamp(long_k_ts);
                    String e_ts = slowLogArr[1];
                    for(int x = 0; x < e_ts.length(); x++) {
                        char c = e_ts.charAt(x);
                        if(c == 'Z' ) {

                            e_ts = e_ts.substring(0, x) + e_ts.substring(x + 1);
                        }
                        else if(c == 'T') {
                            e_ts = e_ts.substring(0,x) + " " + e_ts.substring(x+1);
                        }

                    }
                    //System.out.println(e_ts);

                    Timestamp e_receive_ts = Timestamp.valueOf(e_ts);

                    long ts1 = kafka_ts.getTime();
                    long ts2 = e_receive_ts.getTime();

                    long diff = ts2-ts1;
                    System.out.println("\t"+diff);

                }
            }
        }
        System.out.println("Done matching logs... ");
    }
}
