package org.aghersi;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Instant;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class Main {
    private static final Gson gson = new Gson();
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String KAFKA_GROUP = "anomaly-detector";
    private static final String KAFKA_TOPIC = "augmented";
    private static final String KAFKA_OUTPUT_TOPIC = "anomalies";
    private static final Instant startTime = Instant.now();
    private static final AtomicLong recordCount = new AtomicLong();
    private static final AtomicLong byteCount = new AtomicLong();

    public static void main(String[] args) {
        Properties props = createStreamsConfig();
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(KAFKA_TOPIC);

        processAnomalies(stream);
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        CountDownLatch latch = setupShutdownHook(streams);

        try {
            streams.start();
            latch.await();
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

    private static void processAnomalies(KStream<String, String> stream) {
        stream.peek((key, value) -> logMetrics(value))
                .groupByKey()
                .aggregate(
                        Accumulator::new,
                        Main::accumulateValues,
                        Materialized.with(Serdes.String(), new AccumulatorSerde())
                )
                .toStream()
                .peek((_, acc) -> checkMetrics())
                .filter((_, acc) -> acc.isReady())
                .filter((_, acc) -> isAnomaly(acc))
                .mapValues(Main::createAnomalyJson)
                .to(KAFKA_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static void logMetrics(String value) {
        recordCount.incrementAndGet();
        byteCount.addAndGet(value.getBytes().length);
    }
    private static void checkMetrics() {
        long elapsedTime = Duration.between(startTime, Instant.now()).toSeconds();
        System.out.println("Records Processed: " + recordCount.get() + ", Bytes Processed: " + byteCount.get() + ", Elapsed Time: " + elapsedTime + "s");
    }
    private static Accumulator accumulateValues(String key, String value, Accumulator acc) {
        JsonObject json = gson.fromJson(value, JsonObject.class);
        double adjClose = json.has("adj_close") ? json.get("adj_close").getAsDouble() : 0.0;
        return acc.add(adjClose);
    }

    private static boolean isAnomaly(Accumulator acc) {
        double mean = acc.getMean();
        double stdDev = acc.getStdDev();
        double upperBand = mean + 2 * stdDev;
        double lowerBand = mean - 2 * stdDev;
        double adjClose = acc.getLast();
        return adjClose > upperBand || adjClose < lowerBand;
    }

    private static String createAnomalyJson(Accumulator acc) {
        JsonObject anomaly = new JsonObject();
        anomaly.addProperty("date", Instant.now().toString());
        anomaly.addProperty("adj_close", acc.getLast());
        anomaly.addProperty("method", "MA200");
        anomaly.addProperty("detected_time", Instant.now().toString());
        return anomaly.toString();
    }

    private static CountDownLatch setupShutdownHook(KafkaStreams streams) {
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                System.out.println("Shutdown hook triggered");
                streams.close();
                latch.countDown();
            }
        });
        return latch;
    }
}