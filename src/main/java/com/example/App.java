package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.apache.http.HttpHost;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

public class App {

    public static void main(String[] args) {
        // Configure Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "log-filtering-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Add this to handle offsets
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        // Define the input Kafka topic
        KStream<String, String> inputLogs = builder.stream("input-log-topic");

        // Filter logs that contain "ERROR"
        KStream<String, String> filteredLogs = inputLogs.filter(
                (key, value) -> value != null && value.contains("ERROR"));

        // Elasticsearch client setup
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")))) {

            // Send filtered logs to Elasticsearch
            filteredLogs.foreach((key, logMessage) -> {
                IndexRequest request = new IndexRequest("filtered-log-topic")
                        .id(UUID.randomUUID().toString()) // Unique ID for each document
                        .source("message", logMessage, "level", "ERROR");
                try {
                    client.index(request, RequestOptions.DEFAULT);
                    System.out.println("Log sent to Elasticsearch: " + logMessage);
                } catch (IOException e) {
                    System.err.println("Error indexing log: " + logMessage);
                    e.printStackTrace();
                }
            });

            // Build the topology and start streaming
            KafkaStreams streams = new KafkaStreams(builder.build(), props);
            streams.start();

            // Shutdown hook to close the streams on exit
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down Kafka Streams...");
                streams.close();
                try {
                    client.close();
                } catch (IOException e) {
                    System.err.println("Error closing Elasticsearch client");
                    e.printStackTrace();
                }
            }));

        } catch (IOException e) {
            System.err.println("Failed to initialize Elasticsearch client");
            e.printStackTrace();
        }
    }
}
