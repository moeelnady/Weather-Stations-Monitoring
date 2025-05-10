package org.example;


public class Main {
    public static void main(String[] args) throws Exception {
        CentralBaseStation centralBaseStation = new CentralBaseStation();
        centralBaseStation.consumeMessages();
        new FileHandler().readFileById(1);

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