package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.data.Weather;
import org.example.data.WeatherStatus;

import java.util.Random;

public class WeatherStationStarter {
    private static final Random random = new Random();
    private static final ObjectMapper mapper = new ObjectMapper();
    private static long sequenceNumber = 1;
    private final long stationId;

    public WeatherStationStarter(long station_id){
        this.stationId = station_id;
    }
    public Runnable start(){
        return () -> {
            if (random.nextDouble() < 0.1) {
                System.out.println("Message dropped");
                return;
            }

            WeatherStatus status = new WeatherStatus();
            status.setStation_id(stationId);

            status.setS_no(sequenceNumber++);
            status.setStatus_timestamp(System.currentTimeMillis() / 1000);

            int batteryRand = random.nextInt(100);
            if (batteryRand < 30) status.setBattery_status("low");
            else if (batteryRand < 70) status.setBattery_status("medium");
            else status.setBattery_status("high");

            Weather weather = new Weather();
            weather.setHumidity(20 + random.nextInt(81));      // 20 - 100
            weather.setTemperature(60 + random.nextInt(51));  // 60 - 110
            weather.setWind_speed(5 + random.nextInt(26));     // 5 - 30
            status.setWeather(weather);


            try {
                String json = mapper.writeValueAsString(status);
                ProducerRecord<String, String> record = new ProducerRecord<>("weather-readings", String.valueOf(stationId), json);
                KafkaProducerSingleton.getProducer().send(record);
                System.out.println("Sent: " + json);
            } catch (Exception e) {
                e.printStackTrace();
            }

        };
    }





}
