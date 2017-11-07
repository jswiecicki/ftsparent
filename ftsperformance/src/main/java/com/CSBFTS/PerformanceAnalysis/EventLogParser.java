
package com.CSBFTS.PerformanceAnalysis;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;

public class EventLogParser {

    public static void main(String[] args) {


    }

    public static HashMap<String, Long> parseEventLog() {

        HashMap<String,Long> map = new HashMap<>();
        BufferedReader br = null;

        try {
            FileReader fstream = new FileReader("log.txt");
            br = new BufferedReader(fstream);
            String line;
            while((line = br.readLine()) != null) {
                try {
                    int indexOfID = line.indexOf("uniqueId")+10;
                    int endIndexID = line.indexOf(",\"fname");

                    String uid = line.substring(indexOfID, endIndexID);



                    int indexOfTimestamp = line.indexOf("timeGenerated")+15;
                    int endIndex = line.indexOf("}) meta(");

                    //int indexOfTimestamp = indexOfLabel+15;


                    String ts = line.substring(indexOfTimestamp, endIndex);
                    Long longTimestamp = Long.parseLong(ts);


                    Timestamp t = new Timestamp(longTimestamp);
                    //System.out.println(t.toString());
                    System.out.println("id: "+uid + " timestamp: " +t.toString());


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
