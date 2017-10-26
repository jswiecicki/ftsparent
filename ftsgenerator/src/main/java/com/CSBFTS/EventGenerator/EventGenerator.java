package com.CSBFTS.EventGenerator;

import org.apache.kafka.clients.producer.*;
//import org.apache.kafka.common.serialization.LongSerializer;
//import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
//import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;

import java.util.Properties;

public class EventGenerator {

    private final static String TOPIC = "test3";
    private final static String BOOTSTRAP_SERVERS = "35.196.124.101:9092";

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            runProducer(10); // arg specifies how many times to send a message
        } else {
            runProducer(Integer.parseInt(args[0]));
        }
    }

    private static Producer<String, JsonNode> createProducer() {
        Properties props = new Properties();

        // set the bootstrap server config property to the list of addresses to connect to
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);


        props.put(ProducerConfig.CLIENT_ID_CONFIG, "EventGenerator");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        return new KafkaProducer<String, JsonNode>(props);
    }

    static void runProducer(final int sendMessageCount) throws Exception {
        final Producer<String, JsonNode> producer = createProducer();
        long time = System.currentTimeMillis();


        JsonNode jsonData;
        ObjectMapper mapper = new ObjectMapper(); // costly operation, reuse heavily

        AccountHolder[] accounts = new AccountHolder[sendMessageCount]; // arbitrary length


        for (int i = 0; i < accounts.length; i++) { // populate the accounts field with example data
            accounts[i] = new AccountHolder();
        }

        try {
            for (long index = time; index < time + sendMessageCount; index++) {

                accounts[(int)(index - time)].setTime(System.currentTimeMillis());

                jsonData = mapper.readTree(accounts[(int)(index - time)].toJson());

                final ProducerRecord<String, JsonNode> record = new ProducerRecord<>(TOPIC, jsonData); // creates a record to send
                RecordMetadata metadata = producer.send(record).get(); // sends the record and gathers metadata

                long elapsedTime = System.currentTimeMillis() - time;
                System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
                        record.key(), record.value(), metadata.partition(),
                        metadata.offset(), elapsedTime);

            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

}
