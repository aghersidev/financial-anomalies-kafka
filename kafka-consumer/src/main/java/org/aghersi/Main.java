package org.aghersi;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class Main {
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String KAFKA_GROUP = "anomaly-consumer";
    private static final String KAFKA_TOPIC = "anomalies";
    private static final String RABBITMQ_QUEUE = "anomalies_queue";
    private static final String RABBITMQ_HOST = "localhost";
    private static boolean running = true;

    private static long totalBytesProcessed = 0;
    private static long totalRecordsProcessed = 0;
    private static long isolationForestBytesProcessed = 0;
    private static long isolationForestRecordsProcessed = 0;
    private static long ma200BytesProcessed = 0;
    private static long ma200RecordsProcessed = 0;

    public static void main(String[] args) throws IOException, TimeoutException {

        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP + Instant.now());
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass().getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass().getName());
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBITMQ_HOST);

        Instant startTime = Instant.now();
        System.out.println("Queue reading started at: " + startTime);
        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.queueDeclare(RABBITMQ_QUEUE, true, false, false, null);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutdown initiated");
                running = false;
                try {
                    consumer.close();
                    channel.close();
                } catch (IOException | TimeoutException e) {
                    System.err.println("Error closing: " + e.getMessage());
                }
            }));

            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JsonObject jsonObject = JsonParser.parseString(record.value()).getAsJsonObject();
                        String message = jsonObject.toString();

                        long messageBytes = message.getBytes().length;
                        totalBytesProcessed += messageBytes;
                        totalRecordsProcessed++;

                        String method = jsonObject.has("method") ? jsonObject.get("method").getAsString() : "";
                        if ("Isolation Forest".equals(method)) {
                            isolationForestBytesProcessed += messageBytes;
                            isolationForestRecordsProcessed++;
                        } else if ("MA200".equals(method)) {
                            ma200BytesProcessed += messageBytes;
                            ma200RecordsProcessed++;
                        }

                        channel.basicPublish("", RABBITMQ_QUEUE, null, message.getBytes());
                        System.out.println("Sent to RabbitMQ: " + message);

                    } catch (JsonSyntaxException e) {
                        System.err.println("Invalid JSON: " + record.value());
                    }
                }

                Instant currentTime = Instant.now();
                System.out.println("Total records processed: " + totalRecordsProcessed);
                System.out.println("Total bytes processed: " + totalBytesProcessed);
                System.out.println("Records processed (Isolation Forest): " + isolationForestRecordsProcessed);
                System.out.println("Bytes processed (Isolation Forest): " + isolationForestBytesProcessed);
                System.out.println("Records processed (MA200): " + ma200RecordsProcessed);
                System.out.println("Bytes processed (MA200): " + ma200BytesProcessed);
                System.out.println("Elapsed time since program start: " + Duration.between(startTime, currentTime).toMillis() + " ms");
                System.out.println("Current time: " + currentTime);
            }
            System.out.println("Exiting program");
        } finally {
            consumer.close();
        }
    }
}
