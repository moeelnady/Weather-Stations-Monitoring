package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class FileHandler {
    public void writeEntry(RandomAccessFile file, BitcaskEntry entry) throws IOException {
        file.writeInt(entry.getKey());
        file.writeInt(entry.getDataSize());
        file.writeLong(entry.getTimestamp());
        file.write(entry.getData());
    }

    public BitcaskEntry readEntry(RandomAccessFile file) throws IOException {
        int key = file.readInt();
        int dataSize = file.readInt();
        long timestamp = file.readLong();
        byte[] data = new byte[dataSize];
        file.readFully(data);
        return new BitcaskEntry(key, data,timestamp);
    }
    public void writeHintFile(String path, Map<Integer, FileOffset> index) throws IOException {
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(path))) {
            for (Map.Entry<Integer, FileOffset> entry : index.entrySet()) {
                out.writeInt(entry.getKey());
                out.writeLong(entry.getValue().getFileId());
                out.writeLong(entry.getValue().getOffsetVal());
                out.writeLong(entry.getValue().getTimestamp());
            }
        }
    }

    public Map<Integer, FileOffset> readHintFile(String path) throws IOException {
        Map<Integer, FileOffset> index = new HashMap<>();
        try (DataInputStream in = new DataInputStream(new FileInputStream(path))) {
            while (in.available() > 0) {
                int key = in.readInt();
                long fileId = in.readLong();
                long offset = in.readLong();
                long timestamp = in.readLong();
                index.put(key, new FileOffset(fileId, offset, timestamp));
            }
        }
        return index;
    }
    public void readFileById(int fileId) throws IOException {
        String filePath = "bitcask-data"+"/segment_"+fileId+".data";
        File file = new File(filePath);
        if (!file.exists()) {
            System.out.println("File not found: "+filePath);
            return;
        }

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            ObjectMapper mapper = new ObjectMapper();

            while (raf.getFilePointer() < raf.length()) {
                long offset = raf.getFilePointer();
                BitcaskEntry entry = this.readEntry(raf);

                String json = new String(entry.getData());
                CentralStationReading reading = mapper.readValue(json, CentralStationReading.class);

                System.out.println("Offset: " + offset);
                System.out.println("Key: " + entry.getKey());
                System.out.println("  Station ID: " + reading.station_id);
                System.out.println("  Serial No: " + reading.s_no);
                System.out.println("  Battery: " + reading.battery_status);
                System.out.println("  Timestamp: " + reading.status_timestamp);
                System.out.println("  Weather -> Humidity: " + reading.weather.humidity +
                        ", Temp: " + reading.weather.temperature +
                        ", Wind: " + reading.weather.wind_speed);
                System.out.println("----------------------------------------------------");
            }
        }
    }

}
