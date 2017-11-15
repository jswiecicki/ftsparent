package com.CSBFTS.EventGenerator;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class EventGenerator {
    private ObjectMapper mapper;
    private final static String BOOTSTRAP_SERVERS = "35.196.48.180:9092";

    public EventGenerator(){
        mapper = new ObjectMapper(); // costly operation, reuse heavily
    }

    private static Producer<String, JsonNode> createProducer() {
        Properties props = new Properties();

        // set the bootstrap server config property to the list of addresses to connect to
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);


        props.put(ProducerConfig.CLIENT_ID_CONFIG, "EventGenerator");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        return new KafkaProducer<String, JsonNode>(props);
    }

    public void runProducer(HashMap<Integer, JsonNode> accountDataMap, long intervalBtwnKafkaMsg, BufferedWriter output, String topic) {
        final Producer<String, JsonNode> producer = createProducer();
        long time = System.currentTimeMillis();

        for (Map.Entry<Integer, JsonNode> entry : accountDataMap.entrySet()){
            Integer index = entry.getKey();
            JsonNode jsonData = entry.getValue();

            final ProducerRecord<String, JsonNode> record = new ProducerRecord<>(topic, String.valueOf(index), jsonData); // creates a record to send
            producer.send(record);

            long elapsedTime = System.currentTimeMillis() - time;

            try {
                System.out.print("sent record(key=" +  record.key() +" value=" + record.value() + "), time=" + elapsedTime + "\n");
                output.write("sent record(key=" +  record.key() +" value=" + record.value() + "), time=" + elapsedTime + "\n");
            } catch (IOException e) {
                System.out.println("WHAT!? There was an error writing to the file!");
            }

            try {
                Thread.sleep(intervalBtwnKafkaMsg);
            } catch (InterruptedException e) {
                System.out.println("Oopsie daisy, there was an issue sleeping the thread!");
            }
        }
        try {
            output.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        producer.flush();
        producer.close();

    }

    public void addAccounts(HashMap<Integer, JsonNode> accountDataMap, String topic){
        final Producer<String, JsonNode> producer = createProducer();

        for (Map.Entry<Integer, JsonNode> entry : accountDataMap.entrySet()){
            Integer index = entry.getKey();
            JsonNode jsonData = entry.getValue();

            final ProducerRecord<String, JsonNode> record = new ProducerRecord<>(topic, String.valueOf(index), jsonData); // creates a record to send
            producer.send(record);
        }

        producer.flush();
        producer.close();
    }

    public  HashMap<Integer, JsonNode> createAdd(int amount, String eventType){
        HashMap<Integer, JsonNode> accountDataMap= new HashMap<Integer, JsonNode>();

        for (int i = 1; i <= amount; i++) { // populate the accounts field with example data
            AccountHolder account = new AccountHolder(eventType);
            account.setTime(System.currentTimeMillis());
            try {
                JsonNode jsonData = mapper.readTree(account.toJson());
                accountDataMap.put(i, jsonData);
            } catch (IOException e) {
                System.out.println("Uh oh spagetio! There was an error converting account to JSON!");
            }
        }

        return accountDataMap;
    }


    public void runAddTest(int numOfCreatedAccounts, int qps, String outputFile){
        HashMap<Integer, JsonNode> accountDataMap = createAdd(numOfCreatedAccounts, "add");
        long intervalBtwnKafkaMsg = (long)1000/qps;

        BufferedWriter output = null;
        try {
            output = new BufferedWriter(new FileWriter(outputFile));
        } catch (IOException e) {
            System.out.println("Um wait... We could not open up the file.");
        }

        try {
            runProducer(accountDataMap, intervalBtwnKafkaMsg, output, "test3");
        } catch (Exception e) {
            System.out.println("Mama Mia! There was issue running the producer in runAddTest!");
        }

        ESCleanUp.removeElasticsearchEntries(accountDataMap.keySet());

    }

    public void runUpdateTest(int numOfCreatedAccounts, int qps, String outputFile){
        HashMap<Integer, JsonNode> accountDataMap = createAdd(numOfCreatedAccounts, "update");
        try {
            addAccounts(accountDataMap, "test3");
        } catch (Exception e) {
            System.out.println("Mama Mia! There was issue running the producer in runAddTest!");
        }

        long intervalBtwnKafkaMsg = (long)1000/qps;

        BufferedWriter output = null;
        try {
            output = new BufferedWriter(new FileWriter(outputFile));
        } catch (IOException e) {
            System.out.println("Um wait... We could not open up the file.");
        }

        accountDataMap = createAdd(numOfCreatedAccounts, "update");

        try {
            runProducer(accountDataMap, intervalBtwnKafkaMsg, output, "test3");
        } catch (Exception e) {
            System.out.println("Mama Mia! There was issue running the producer in runAddTest!");
        }

        ESCleanUp.removeElasticsearchEntries(accountDataMap.keySet());
    }
}
