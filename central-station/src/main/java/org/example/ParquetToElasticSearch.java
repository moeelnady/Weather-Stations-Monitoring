package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class ParquetToElasticSearch {
    public void parquetToJson() throws IOException {
        String parquetDir = "parquet-data";
        String indexName = "weather-statuses";
        String esHost = "localhost";
        int esPort = 9200;
        String username = "elastic";
        String password = "changeme";

        // Setup Elasticsearch client with basic authentication
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http"))
        );
        ObjectMapper mapper = new ObjectMapper();

        File folder = new File(parquetDir);
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".parquet"));

        if (files == null || files.length == 0) {
            System.out.println("No parquet files found in: " + parquetDir);
            esClient.close();
            return;
        }

        for (File file : files) {
            System.out.println("Indexing: " + file.getName());

            Path parquetPath = new Path(file.getAbsolutePath());
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(parquetPath)
                    .withConf(new Configuration())
                    .build();

            GenericRecord record;
            while ((record = reader.read()) != null) {
                try {
                    // Convert to valid JSON
                    JsonNode jsonNode = mapper.readTree(record.toString());
                    Map<String, Object> map = mapper.convertValue(jsonNode, Map.class);

                    // Index into Elasticsearch
                    IndexRequest request = new IndexRequest(indexName).source(map);
                    try {
                        esClient.index(request, RequestOptions.DEFAULT);
                    } catch (Exception e) {
                        if (e.getMessage().contains("201 Created")) {

                            System.out.println("Record indexed ");
                        } else {
                            System.err.println("Failed to index record: " + e.getMessage());
                        }
                    }

                } catch (Exception e) {
                    System.err.println("Failed to index record: " + e.getMessage());
                }
            }

            reader.close();
        }

        esClient.close();
        System.out.println("All Parquet files indexed successfully.");
    }
}
