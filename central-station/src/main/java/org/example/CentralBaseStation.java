package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;


public class CentralBaseStation {

    public void consumeMessages() throws Exception {
        // TODO: write to bitcaskfile
        String topic = "raining-readings";
        // BitcaskEngine bitcask = new BitcaskEngine();
        ObjectMapper mapper = new ObjectMapper();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-base-station");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic));
        try{
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String json = record.value();
                    try {
                        CentralStationReading reading = mapper.readValue(json, CentralStationReading.class);
                        int key = reading.station_id;
                        byte[] valueBytes = json.getBytes(); // Store full raw JSON
                        // bitcask.put(key, valueBytes);
                        System.out.println("Saved station " + key + " to BitCask");
                    } catch (Exception e) {
                        System.err.println("Failed to process message: " + e.getMessage());
                    }
                }
            }

        } finally {
            consumer.close();
        }
        //TODO: Write records in parquet



    }

}
