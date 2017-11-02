package com.CSBFTS.EventGenerator;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class EventGenerator {
    private ObjectMapper mapper;
    private final static String TOPIC = "test3";
    private final static String BOOTSTRAP_SERVERS = "35.196.124.101:9092";

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


    public void runProducer(HashMap<Integer, JsonNode> accountDataMap) {
        final Producer<String, JsonNode> producer = createProducer();

        long time = System.currentTimeMillis();

            try {
                for (Map.Entry<Integer, JsonNode> entry : accountDataMap.entrySet()){
                    Integer index = entry.getKey();
                    JsonNode jsonData = entry.getValue();

                    final ProducerRecord<String, JsonNode> record = new ProducerRecord<>(TOPIC, String.valueOf(index), jsonData); // creates a record to send
                    RecordMetadata metadata = producer.send(record).get();

                    long elapsedTime = System.currentTimeMillis() - time;
                    System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
                            record.key(), record.value(), metadata.partition(),
                            metadata.offset(), elapsedTime);
                }

            } catch (InterruptedException e) {
                //add error messages
                e.printStackTrace();
            } catch (ExecutionException e) {
                //add error messages
                e.printStackTrace();
            } finally{
                producer.flush();
                producer.close();
            }
    }

    public  HashMap<Integer, JsonNode> createAdd(int amount){
        HashMap<Integer, JsonNode> accountDataMap= new HashMap<Integer, JsonNode>();

        for (int i = 1; i <= amount; i++) { // populate the accounts field with example data
            AccountHolder account = new AccountHolder();
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


    public void runAddTest(int amount){
        HashMap<Integer, JsonNode> accountDataMap = createAdd(amount);
        try {
            runProducer(accountDataMap);
        } catch (Exception e) {
            System.out.println("Mama Mia! There was issue running the producer in runAddTest!");
        }

        ESCleanUp.removeElasticsearchEntries(accountDataMap.keySet());

    }
}
