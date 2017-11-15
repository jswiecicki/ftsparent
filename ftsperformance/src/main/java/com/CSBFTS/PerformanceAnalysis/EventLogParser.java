
package com.CSBFTS.PerformanceAnalysis;


import java.io.*;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;

public class EventLogParser {

    public static void main(String[] args) {
        HashMap<String, Long> map = parseEventLog();

        Iterator i = map.entrySet().iterator();
        while(i.hasNext()) {
            HashMap.Entry pair = (HashMap.Entry)i.next();
            System.out.println("id: "+pair.getKey() + " timestamp: " + pair.getValue());
        }

    }

    public static HashMap<String, Long> parseEventLog() {

        HashMap<String,Long> map = new HashMap<>();
        BufferedReader br = null;

        try {
            //FileWriter fileWriter = new FileWriter("event_log_keys.txt");
            BufferedWriter bufferedWriter  = new BufferedWriter(new FileWriter("event_log_keys.txt"));

            //PrintWriter pw = new PrintWriter("event_log_keys.txt","UTF-8");

            FileReader fstream = new FileReader("event_gen_output.txt");
            br = new BufferedReader(fstream);
            String line;
            while((line = br.readLine()) != null) {
                try {
                    int indexOfID = line.indexOf("uniqueId")+10;
                    int endIndexID = line.indexOf(",\"fname");

                    String uid = line.substring(indexOfID, endIndexID);

                    int indexOfTimestamp = line.indexOf("timeGenerated")+15;
                    int endIndex = line.indexOf(",\"eventType\"");

                    int indexOfEventType=line.indexOf("eventType")+12;
                    int endIndexOfEventType=line.indexOf("\"}), time");

                    String eventType = line.substring(indexOfEventType, endIndexOfEventType);

                    String ts = line.substring(indexOfTimestamp, endIndex);
                    Long longTimestamp = Long.parseLong(ts);

                    Timestamp t = new Timestamp(longTimestamp);

                    bufferedWriter.write(uid+"\t"+longTimestamp.toString()+"\n");
                    map.put(uid, longTimestamp);

                    //System.out.println("uid: "+uid+" timestamp: " +t.toString());


                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
        return map;
    }
}
