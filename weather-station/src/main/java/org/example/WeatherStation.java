package org.example;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WeatherStation {
    public static void main(String[] args) {
        ScheduledExecutorService scheduler =
                Executors.newScheduledThreadPool(10);
        for(int i =1; i <=10; i++){
            WeatherStationStarter weatherStationStarter =
                    new WeatherStationStarter(i);
            scheduler.scheduleAtFixedRate
                    (weatherStationStarter.start(), 0, 1, TimeUnit.SECONDS);
        }


    }
}