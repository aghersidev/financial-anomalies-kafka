package org.aghersi;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Main {
    private static final Gson gson = new Gson();
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String KAFKA_GROUP = "stock-processor-app";
    private static final String KAFKA_TOPIC = "input-data";
    private static final String KAFKA_OUTPUT_TOPIC = "augmented";

    public static void main(String[] args) {
        Properties props = createStreamsConfig();
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(KAFKA_TOPIC);

        KStream<String, String> processedStream = processStream(source);
        processedStream.to(KAFKA_OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        CountDownLatch latch = setupShutdownHook(streams);

        try {
            streams.start();
            latch.await(); // Wait for shutdown hook
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static Properties createStreamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KAFKA_GROUP);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    private static KStream<String, String> processStream(KStream<String, String> source) {
        return source.mapValues(value -> {
            JsonObject json = gson.fromJson(value, JsonObject.class);
            double high = json.get("high").getAsDouble();
            double low = json.get("low").getAsDouble();
            double open = json.get("open").getAsDouble();
            double close = json.get("close").getAsDouble();

            json.addProperty("up", high - Math.max(open, close));
            json.addProperty("down", Math.min(open, close) - low);
            json.addProperty("size", Math.abs(close - open));

            return gson.toJson(json);
        });
    }

    private static CountDownLatch setupShutdownHook(KafkaStreams streams) {
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
        return latch;
    }
}
