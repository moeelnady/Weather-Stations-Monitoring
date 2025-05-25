package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class parquetEngine {
    private final String outputDir;
    private final SparkSession spark;
    private final ObjectMapper mapper = new ObjectMapper();

    public parquetEngine(String outputDir) {
        this.outputDir = outputDir;
        try {
            Files.createDirectories(Path.of(outputDir));
        } catch (FileAlreadyExistsException e) {
            System.out.println("path is not a directory.");
            System.exit(-1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.setProperty("HADOOP_USER_NAME", "anonymous");

        this.spark = SparkSession.builder()
                .appName("ParquetWriterApp")
                .master("local[*]")
                .config("spark.hadoop.hadoop.security.authentication", "simple")
                .config("spark.hadoop.hadoop.security.authorization", "false")
                .config("spark.hadoop.fs.hdfs.impl.disable.cache", "true")
                .getOrCreate();
    }

    public void writeBatch(List<String> buffer) {
        Map<String, List<String>> stationGroups = new HashMap<>();
        for (String json : buffer) {
            try {
                CentralStationReading reading = mapper.readValue(json, CentralStationReading.class);
                stationGroups.computeIfAbsent(String.valueOf(reading.station_id), k -> new ArrayList<>()).add(json);
            } catch (Exception e) {
                System.err.println("Failed to parse JSON: " + e.getMessage());
            }
        }

        String currentTime = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS").format(new Date());

        for (Map.Entry<String, List<String>> entry : stationGroups.entrySet()) {
            String stationId = entry.getKey();
            List<String> records = entry.getValue();

            // Temp directory to write parquet
            String tempDir = outputDir + "/temp_station_" + stationId + "_" + currentTime;
            String finalFileName = outputDir + "/station_" + stationId + "_" + currentTime + ".parquet";

            try {
                Dataset<String> dataset = spark.createDataset(records, Encoders.STRING());
                Dataset<Row> df = spark.read().json(dataset);

                // Write exactly one part file to temporary directory
                df.coalesce(1)
                        .write()
                        .mode("overwrite")
                        .parquet(tempDir);

                // Find the single part file in the temp directory
                File dir = new File(tempDir);
                File[] files = dir.listFiles((d, name) -> name.startsWith("part-") && name.endsWith(".parquet"));

                if (files != null && files.length == 1) {
                    File partFile = files[0];


                    Files.move(partFile.toPath(), Paths.get(finalFileName), StandardCopyOption.REPLACE_EXISTING);


                    for (File f : Objects.requireNonNull(dir.listFiles())) {
                        f.delete();
                    }
                    dir.delete();

                    System.out.println("Written " + records.size() + " records to file: " + finalFileName);
                } else {
                    System.err.println("Unexpected number of part files found: " + (files == null ? 0 : files.length));
                }
            } catch (Exception e) {
                System.err.println("Failed to write for station " + stationId + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

}
