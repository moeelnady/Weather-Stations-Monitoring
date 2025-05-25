package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.bitcask.Bitcask;
import org.example.bitcask.net.BitcaskServer;

import java.time.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CentralBaseStation {
    private static final int BATCH_SIZE = 100;
    private final List<String> buffer = new ArrayList<>();
    private final Object lock = new Object();
    parquetEngine parquetEng = new parquetEngine("./parquet-data/");
    ParquetToElasticSearch parquetToElasticSearch = new ParquetToElasticSearch();

    public void consumeMessages() throws Exception {
        String topic = "weather-readings";
        Bitcask bitcask = new Bitcask("./bitcask-data/");
        bitcask.scheduleCompaction();

        BitcaskServer server = null;
        Thread serverThread = null;
        try {
            server = new BitcaskServer(bitcask, 1022);
            serverThread = new Thread(server::start);
            serverThread.start();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("failed to start the server");
        }

        ObjectMapper mapper = new ObjectMapper();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-base-station");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String json = record.value();
                    try {
                        CentralStationReading reading = mapper.readValue(json, CentralStationReading.class);
                        String key = Integer.toString(reading.station_id);
                        byte[] valueBytes = json.getBytes(); // Store full raw JSON
                        bitcask.put(key, valueBytes);
                        System.out.println("Saved station " + key + " to BitCask");
                    } catch (Exception e) {
                        System.err.println("Failed to process message: " + e.getMessage());
                    }
                    try {
                        synchronized (lock) {
                            buffer.add(json);
                            if (buffer.size() >= BATCH_SIZE) {
                                System.out.println("start parquet");
                                parquetEng.writeBatch(buffer);
                                parquetToElasticSearch.parquetToJson();
                                buffer.clear();
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Failed to process message: " + e.getMessage());
                    }
                }
            }

        } finally {
            consumer.close();
            if (server != null) server.close();
        }
    }
}
