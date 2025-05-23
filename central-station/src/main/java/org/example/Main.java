package org.example;

import java.nio.file.Path;
import java.util.Arrays;

import org.example.bitcask.Bitcask;

public class Main {
    public static void main(String[] args) throws Exception {
        Bitcask x = new Bitcask(Path.of("../bitcask-data/"));
        // x.put("2", new byte[] {3});
        // x.put("4", new byte[] {5});
        // x.put("6", new byte[] {7});
        // x.put("8", new byte[] {9});
        // x.put("7", new byte[] {8});
        // x.put("10", new byte[] {10});
        // x.put("40", new byte[] {40});
        // x.put("52", new byte[] {52});
        // x.put("60", new byte[] {60});
        // x.put("72", new byte[] {72});
        // x.put("84", new byte[] {84});
        // x.put("96", new byte[] {96});
        // x.compact();
        System.out.print(Arrays.toString(x.get("2")));
        System.out.print(Arrays.toString(x.get("4")));
        System.out.print(Arrays.toString(x.get("6")));
        System.out.print(Arrays.toString(x.get("8")));
        System.out.print(Arrays.toString(x.get("7")));
        System.out.print(Arrays.toString(x.get("10")));
        System.out.print(Arrays.toString(x.get("40")));
        System.out.print(Arrays.toString(x.get("52")));
        System.out.print(Arrays.toString(x.get("60")));
        System.out.print(Arrays.toString(x.get("72")));
        System.out.print(Arrays.toString(x.get("84")));
        System.out.print(Arrays.toString(x.get("96")));
    }
    /*public static void readAndPrintAllIndex(BitcaskEngine bitcask) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        for (Integer key : bitcask.getAllKeys()) {
            String json = bitcask.get(key); // Already returns JSON as string
            if (json != null) {
                CentralStationReading reading = mapper.readValue(json, CentralStationReading.class);
                System.out.println("Station ID: " + reading.station_id);
                System.out.println("  Serial No: " + reading.s_no);
                System.out.println("  Battery: " + reading.battery_status);
                System.out.println("  Timestamp: " + reading.status_timestamp);
                System.out.println("  Weather -> Humidity: " + reading.weather.humidity +
                        ", Temp: " + reading.weather.temperature +
                        ", Wind: " + reading.weather.wind_speed);
                System.out.println("============================================");
            }
        }
    }*/


}
