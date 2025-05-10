package org.example;

public class CentralStationReading {
    public int station_id;
    public int s_no;
    public String battery_status;
    public long status_timestamp;
    public Weather weather;

    public static class Weather {
        public int humidity;
        public int temperature;
        public int wind_speed;
    }
}