package com.CSBFTS.EventGenerator;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
//import org.apache.kafka.connect.json.JsonSerializer;

import com.google.gson.Gson;

import java.util.Properties;

public class EventGenerator {

    private final static String TOPIC = "demo5";
    private final static String BOOTSTRAP_SERVERS = "108.59.82.145:9092";

    public static void main(String[] args) throws Exception {

		/*Event sampleEvent = new Event();
		Event sampleEvent2 = new Event();
		System.out.println(sampleEvent.toString());
		System.out.println(sampleEvent2.toString());*/

        if (args.length == 0) {
            runProducer(1);
        } else {
            runProducer(Integer.parseInt(args[0]));
        }
    }

    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();

        // set the bootstrap server config property to the list of addresses to connect to
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);


        props.put(ProducerConfig.CLIENT_ID_CONFIG, "EventGenerator");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    static void runProducer(final int sendMessageCount) throws Exception {
        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();

        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                String jsonData = "{\"name\":\"Test log\", \"severity\": \"INFO\"}";
                Event sample = new Event();

//	        	  	Gson gson = new Gson();

//	        	  	String jsonData = gson.toJson(sample);

                //jsonData.replaceAll("\0", "");

                final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, index, jsonData);

                RecordMetadata metadata = producer.send(record).get();

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
