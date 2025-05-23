package org.example;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WeatherStation {
    public static void main(String[] args) {
        long stationId = 1;
        String stationNum = System.getenv("stationId");
        System.out.println("stationNumber: " + stationNum);
        try {
            stationId = Long.parseLong(stationNum);
        } catch (NumberFormatException e) {
            System.err.println("Invalid station ID. Using default: 1");
        }

        WeatherStationStarter starter = new WeatherStationStarter(stationId);
        Runnable task = starter.start();

        while (true) {
            task.run();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }
    }
}