package com.CSBFTS.PerformanceAnalysis;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;

import java.net.*;

import java.util.Properties;
//import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.Collections;
import java.util.Properties;

import java.io.*;


public class ConsumerRunner {
    //private final static String TOPIC = "my-topic";
    //private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private final static String TOPIC = "sprint2demo";
    private final static String BOOTSTRAP_SERVERS = "108.59.82.145:9092";

    public static void main(String[] args) throws Exception {

        //runConsumer();
        printLog();

        elasticConnection();
    }

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());


        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer<Long, String>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    public static void runConsumer() throws InterruptedException {
        final Consumer<Long, String> consumer = createConsumer();

        final int giveUp = 100;
        int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                try {
                    writeToLog(record.value());
                }
                catch (IOException ex) {
                    ex.printStackTrace();
                }
            });
            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }

    public static void writeToLog(String str) throws IOException {
        BufferedWriter bw = null;
        try {
            FileWriter fstream = new FileWriter("log.txt", true);
            bw = new BufferedWriter(fstream);
            bw.write(str, 0, str.length());
            bw.write("\n",0,1);
        }
        catch (IOException ex) {
            System.out.println("Exception thrown while creating log file");
            ex.printStackTrace();
        }
        finally {
            if (bw != null) {
                bw.close();
            }
        }
    }

    public static void printLog() {
        BufferedReader in = null;
        try {
            FileReader fstream = new FileReader("log.txt");
            in = new BufferedReader(fstream);
            String line;
            while((line=in.readLine()) != null) {
                //String line = in.readLine();
                System.out.println(line);
            }
        }
        catch (IOException ex) {
            System.out.println("Exception thrown while creating log file");
            ex.printStackTrace();
        }
        finally {
            if (in != null) {
                try{
                    in.close();
                } catch(IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    @SuppressWarnings("resource")
    public static void elasticConnection() {
        try{
            Settings settings = Settings.builder()
                    .put("cluster.name", "elasticsearch-cluster")
                    //.put("client.transport.sniff", true)
                    //.put("shield.user", elasticUserName+":"+elasticPassword)
                    .build();

            TransportClient client = TransportClient.builder().settings(settings).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("104.197.66.52"), 9300));

            String json = "{" +
                    "\"user\":\"ben\"," +
                    "\"postDate\":\"2017-09-29\"," +
                    "\"message\":\"TEST\"" +
                    "}";

            IndexResponse response = client.prepareIndex("demo", "tweet")
                    .setSource(json)
                    .get();

            System.out.println(response.isCreated());

            client.close();
        } catch (UnknownHostException ex) {
            System.out.println("Unknown host exception");
            ex.printStackTrace();
        }
    }
}
