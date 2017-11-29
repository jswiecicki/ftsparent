package com.CSBFTS.EventGenerator;

import com.CSBFTS.Config.ServerConfig;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;


public class EventGenerator {
    private ObjectMapper mapper;
    private final static String BOOTSTRAP_SERVERS = "35.196.104.252:9092";
    private final static String BULK_INDEX = "accounts_index";
    private final static String BULK_TYPE = "accountnew";
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");


    public EventGenerator(){
        mapper = new ObjectMapper(); // costly operation, reuse heavily
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
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

    public void runProducer(HashMap<Integer, AccountHolder> accountDataMap, long intervalBtwnKafkaMsg, String topic) {
        final Producer<String, JsonNode> producer = createProducer();
        long sentTime = 0;


        for (Map.Entry<Integer, AccountHolder> entry : accountDataMap.entrySet()){
            Integer index = entry.getKey();
            AccountHolder accountData = entry.getValue();


            accountData.setTime(sdf.format(System.currentTimeMillis()));
            JsonNode jsonData = null;

            try {
                jsonData = mapper.readTree(accountData.toJson());
            } catch (IOException e) {
                e.printStackTrace();
            }


            final ProducerRecord<String, JsonNode> record = new ProducerRecord<>(topic, String.valueOf(index), jsonData); // creates a record to send
            producer.send(record);


            System.out.print("sent record(key=" +  record.key() +" value=" + record.value() + "), time=" + sentTime + "\n");


            try {
                Thread.sleep(intervalBtwnKafkaMsg);
            } catch (InterruptedException e) {
                System.out.println("Oopsie daisy, there was an issue sleeping the thread!");
            }
        }

        producer.flush();
        producer.close();

    }

    public void addAccounts(HashMap<Integer, AccountHolder> accountDataMap, String topic){
        final Producer<String, JsonNode> producer = createProducer();


        for (Map.Entry<Integer, AccountHolder> entry : accountDataMap.entrySet()){
            Integer index = entry.getKey();
            AccountHolder accountData = entry.getValue();

            accountData.setTime(sdf.format(System.currentTimeMillis()));
            JsonNode jsonData = null;
            try {
                jsonData = mapper.readTree(accountData.toJson());
            } catch (IOException e) {
                e.printStackTrace();
            }

            final ProducerRecord<String, JsonNode> record = new ProducerRecord<>(topic, String.valueOf(index), jsonData); // creates a record to send
            producer.send(record);
        }

        producer.flush();
        producer.close();
    }

    public  HashMap<Integer, AccountHolder> createAdd(int amount, String eventType){
        HashMap<Integer, AccountHolder> accountDataMap= new HashMap<Integer, AccountHolder>();

        for (int i = 1; i <= amount; i++) { // populate the accounts field with example data
            AccountHolder account = new AccountHolder(eventType);
            accountDataMap.put(i, account);
        }

        return accountDataMap;
    }

    //Adds acounts in 25,000 account increments
    public void bulkAddAccounts(int amount) {
        HashMap<Integer,AccountHolder> accountDataMap;
        RestClient client  = RestClient.builder(
                new HttpHost(ServerConfig.ELASTICSEARCH_IP, 9200, "http")).build();

        RestHighLevelClient hlClient = new RestHighLevelClient(client);

        accountDataMap = createAdd(amount, "add");
        BulkRequest request = new BulkRequest();
        for(int i = 1; i <= amount; i++) {
            String accountDataJson = null;
            try {
               accountDataJson = accountDataMap.get(i).toJson();
            } catch (IOException e) {
                e.printStackTrace();
            }
            request.add(new IndexRequest(BULK_INDEX, BULK_TYPE, String.valueOf(i)).source(accountDataJson, XContentType.JSON));
            if(i % 25000 == 0) {
                BulkResponse bulkResponse = null;
                try {
                    bulkResponse = hlClient.bulk(request);
                    if(bulkResponse.hasFailures()) {
                        for (BulkItemResponse bulkItemsResponse : bulkResponse){
                            if (bulkItemsResponse.isFailed()) {
                                BulkItemResponse.Failure failure = bulkItemsResponse.getFailure();
                                System.out.println(failure.toString());
                            }
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                request = new BulkRequest();
            }
        }
    }


    public void runAddTest(int numOfCreatedAccounts, int qps){
        HashMap<Integer, AccountHolder> accountDataMap = createAdd(numOfCreatedAccounts, "add");
        long intervalBtwnKafkaMsg = (long)1000/qps;


        try {
            runProducer(accountDataMap, intervalBtwnKafkaMsg, "test6");
        } catch (Exception e) {
            System.out.println("Mama Mia! There was issue running the producer in runAddTest!");
        }

        ESCleanUp.removeElasticsearchEntries(accountDataMap.keySet());

    }

    public void runUpdateTest(int numOfCreatedAccounts, int qps){
        HashMap<Integer, AccountHolder> accountDataMap = createAdd(numOfCreatedAccounts, "add");
        try {
            addAccounts(accountDataMap, "test6");
        } catch (Exception e) {
            System.out.println("Mama Mia! There was issue running the producer in runAddTest!");
        }

        long intervalBtwnKafkaMsg = (long)1000/qps;


        accountDataMap = createAdd(numOfCreatedAccounts, "update");

        try {
            runProducer(accountDataMap, intervalBtwnKafkaMsg, "test6");
        } catch (Exception e) {
            System.out.println("Mama Mia! There was issue running the producer in runAddTest!");
        }

        ESCleanUp.removeElasticsearchEntries(accountDataMap.keySet());
    }

    public void runDeleteTest(int numOfCreatedAccounts, int qps){
        HashMap<Integer, AccountHolder> accountDataMap = createAdd(numOfCreatedAccounts, "add");
        try {
            addAccounts(accountDataMap, "test6");
        } catch (Exception e) {
            System.out.println("Mama Mia! There was issue running the producer in runAddTest!");
        }

        long intervalBtwnKafkaMsg = (long)1000/qps;

        try {
            runProducer(accountDataMap, intervalBtwnKafkaMsg, "deletetest");
        } catch (Exception e) {
            System.out.println("Mama Mia! There was issue running the producer in runAddTest!");
        }

    }
}
