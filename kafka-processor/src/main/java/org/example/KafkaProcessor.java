package org.example;


import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaProcessor {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "raining-trigger-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Stream from weather-readings topic
        KStream<String, String> source = builder.stream("weather-readings");
        KStream<String, String> raining = source.filter((key, value) -> {
            try {
                JsonObject json = JsonParser.parseString(value).getAsJsonObject();
                JsonObject weather = json.getAsJsonObject("weather");
                int humidity = weather.get("humidity").getAsInt();
                return humidity > 70;
            } catch (Exception e) {
                System.err.println("Invalid message: " + value);
                return false;
            }
        });
        // Send to new topic
        raining.to("raining-readings");
        // Start streaming
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}