import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.*;
import java.net.http.*;
import java.net.URI;
import java.time.Instant;
import java.util.*;

public class OpenMeteoToKafka {

    private static final String TOPIC = "weather-readings2";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String OPEN_METEO_API = "https://api.open-meteo.com/v1/forecast?latitude=30.06&longitude=31.25&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m";

    public static void main(String[] args) throws Exception {
        // Set up Kafka producer
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        // Fetch data from Open-Meteo
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(OPEN_METEO_API))
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        String json = response.body();

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json);

        JsonNode timeArray = root.at("/hourly/time");
        JsonNode tempArray = root.at("/hourly/temperature_2m");
        JsonNode humidityArray = root.at("/hourly/relative_humidity_2m");
        JsonNode windArray = root.at("/hourly/wind_speed_10m");

        int recordCount = Math.min(Math.min(tempArray.size(), humidityArray.size()), windArray.size());

        for (int i = 0; i < recordCount; i++) {
            long timestamp = Instant.parse(timeArray.get(i).asText()).getEpochSecond();

            // Build the message according to your schema
            Map<String, Object> weatherMap = new HashMap<>();
            weatherMap.put("temperature", tempArray.get(i).asInt());
            weatherMap.put("humidity", humidityArray.get(i).asInt());
            weatherMap.put("wind_speed", windArray.get(i).asInt());

            Map<String, Object> finalMessage = new HashMap<>();
            finalMessage.put("station_id", 7);  // Set your station ID
            finalMessage.put("s_no", i + 1);     // Simulated sensor reading number
            finalMessage.put("battery_status", (i % 5 == 0) ? "low" : "ok"); // simulate
            finalMessage.put("status_timestamp", timestamp);
            finalMessage.put("weather", weatherMap);

            String messageJson = mapper.writeValueAsString(finalMessage);

            // Send to Kafka
            producer.send(new ProducerRecord<>(TOPIC, messageJson), (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Message sent: " + messageJson);
                } else {
                    exception.printStackTrace();
                }
            });
        }

        producer.close();
    }
}
