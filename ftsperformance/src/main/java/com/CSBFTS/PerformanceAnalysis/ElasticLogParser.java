package com.CSBFTS.PerformanceAnalysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

public class ElasticLogParser {

    public static void main(String[] args) {
        parse();
    }
    public static ArrayList<String[]> parse() {

        String indicators[] = {"\"receiveTimestamp\":\"", "\",\"resource\"",
                "took[", "], took_millis[",
                "\"timestamp\":\"", "\\\"uniqueId\\\""};

        int a, b, x, y, z, c, d, exponent = 0;

        String line;

        ArrayList<String[]> toReturn = new ArrayList<>();

        // change the filename string inside the fileReader constructor when using a different file
        try (BufferedReader reader = new BufferedReader(new FileReader("logFileExample.json"));
             PrintWriter output = new PrintWriter(new File("timeData.csv"));
        ){

            StringBuilder logLine = new StringBuilder();

            // formatting for the csv file
            logLine.append("Received Timestamp");
            logLine.append(',');
            logLine.append("Time Taken");
            logLine.append(',');
            logLine.append("Sent Timestamp");
            logLine.append('\n');

            while ((line = reader.readLine()) != null) {
                a = line.indexOf(indicators[0]) + 20; // beginning of the receiveTimestamp
                b = line.indexOf(indicators[1]);
                x = line.indexOf(indicators[2]) + 5; // beginning of the time taken
                y = line.indexOf(indicators[3]);
                z = line.indexOf(indicators[4]) + 13; // beginning of the sent timestamp
                c = line.indexOf(indicators[5])+13; //beginning of uniqueId

                // data format check
                if (a == 19 || b == -1 || x == 4 || y == -1 || z == 12 || c == 12) {
                    System.out.println("Error: data not in correct format");
                    // must close as try-with-resources statement won't close with a sys exit call
                    output.close();
                    System.exit(-1);
                }

                if (line.substring(x,y).charAt(line.substring(x,y).length() - 2) == 'm') {
                    y -= 2;
                    exponent = -3;
                }
                else if (line.substring(x,y).charAt(line.substring(x,y).length() - 2) == 'o') {
                    y -= 6;
                    exponent = -6;
                }

                d= line.indexOf("}]\\n\",\"timestamp");
                //System.out.println(line.substring(c,d));

                String items[] = {line.substring(c, d), line.substring(a, b), line.substring(x, y)+"e"+exponent, line.substring(z,line.length()-2)};
                toReturn.add(items);

                // print out the captured data
                /*
                System.out.println("Received Timestamp: " + line.substring(a, b) +
                        "\tTime Taken: " + line.substring(x, y) + "e" + exponent +
                        "\tSent Timestamp: " + line.substring(z, line.length() - 2) +
                        "\tUnique ID: " + line.substring(c,d));
                */
                logLine.append(line.substring(a, b));
                logLine.append(',');
                logLine.append(line.substring(x, y));
                logLine.append("E");
                logLine.append(exponent);
                logLine.append(',');
                logLine.append(line.substring(z, line.length() - 2));
                logLine.append(',');
                logLine.append(line.substring(c,d));
                logLine.append('\n');

            }

            output.write(logLine.toString());
            return toReturn;
        }
        catch (IOException e) {
            System.out.println("You should check that the fileReader is given the right file location in the code");
            e.printStackTrace();
        }
        return toReturn;
    }

}
