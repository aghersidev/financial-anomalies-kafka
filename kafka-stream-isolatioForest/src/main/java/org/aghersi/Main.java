package org.aghersi;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import smile.anomaly.IsolationForest;

import java.time.Instant;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class Main {
    private static final Gson gson = new Gson();
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String KAFKA_GROUP = "anomaly-tree-detector";
    private static final String KAFKA_TOPIC = "augmented";
    private static final String KAFKA_OUTPUT_TOPIC = "anomalies";
    private static final int ROLLING_WINDOW_SIZE = 100;
    private static final int FIT_FREQUENCY = 1000;  // Fit the model every 100 messages
    private static final Map<String, List<double[]>> featureVectorsMap = new HashMap<>();
    private static final Map<String, IsolationForest> isolationForestMap = new HashMap<>();
    private static final Map<String, List<Double>> anomalyScoresMap = new HashMap<>();
    private static final Instant startTime = Instant.now();
    private static final AtomicLong recordCount = new AtomicLong();
    private static final AtomicLong byteCount = new AtomicLong();
    private static final AtomicLong cantAnomalies = new AtomicLong();

    public static void main(String[] args) {
        Properties props = createStreamsConfig();
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(KAFKA_TOPIC);

        stream
                .map(Main::mapToAnomalyScore)
                .filter(Main::filterAnomalies)
                .mapValues(Main::mapToAnomalyJson)
                .peek(Main::logMetrics)
                .to(KAFKA_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KAFKA_GROUP );
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    private static KeyValue<String, JsonObject> mapToAnomalyScore(String key, String value) {
        recordCount.incrementAndGet();
        byteCount.addAndGet(value.getBytes().length);

        JsonObject json = gson.fromJson(value, JsonObject.class);

        double adjClose = json.has("adj_close") ? json.get("adj_close").getAsDouble() : 0.0;
        double volume = json.has("volume") ? json.get("volume").getAsDouble() : 0.0;
        double up = json.has("up") ? json.get("up").getAsDouble() : 0.0;
        double down = json.has("down") ? json.get("down").getAsDouble() : 0.0;
        double size = json.has("size") ? json.get("size").getAsDouble() : 0.0;
        double[] features = {adjClose, volume, up, down, size};

        featureVectorsMap.putIfAbsent(key, new ArrayList<>());
        anomalyScoresMap.putIfAbsent(key, new ArrayList<>());
        isolationForestMap.putIfAbsent(key, new IsolationForest(30, 1));

        List<double[]> keyFeaturesList = featureVectorsMap.get(key);
        System.out.println("Is it adding: " + featureVectorsMap.get(key).size());
        keyFeaturesList.add(features);
        System.out.println("Is it adding: " + featureVectorsMap.get(key).size());

        if (keyFeaturesList.size() > ROLLING_WINDOW_SIZE) {
            //keyFeaturesList.removeFirst();
            IsolationForest trainedForest = isolationForestMap.get(key).fit(keyFeaturesList.toArray(new double[0][]));
            System.out.println("Trained with n trees: " + trainedForest.size());
            isolationForestMap.put(key, trainedForest);
            keyFeaturesList.clear();
        }
        System.out.println("After check rolling: " + featureVectorsMap.get(key).size());

        if (keyFeaturesList.size() % FIT_FREQUENCY == 0) {

        } else {

        }

        double score = isolationForestMap.get(key).score(features);
        List<Double> scores = anomalyScoresMap.get(key);
        System.out.println(score);
        if (!Double.isNaN(score)) {
            scores.add(score);
        }

        if (scores.size() > ROLLING_WINDOW_SIZE) {
            scores.removeFirst();
        }

        double dynamicThreshold = calculateDynamicThreshold(key);

        JsonObject result = new JsonObject();
        result.addProperty("score", score);
        result.addProperty("adj_close", adjClose);
        result.addProperty("dynamic_threshold", dynamicThreshold);
        System.out.println("How many tress: " + isolationForestMap.get(key).size());
        System.out.println("How many features arrays: " + keyFeaturesList.size());

        System.out.println(result);
        return new KeyValue<>(key, result);
    }

    private static double calculateDynamicThreshold(String key) {
        double dynamicThreshold = 0;
        if (anomalyScoresMap.get(key).size() >= 2) {
            anomalyScoresMap.get(key).removeIf(score -> Double.isNaN(score));
            double mean = anomalyScoresMap.get(key).stream()
                    .mapToDouble(Double::doubleValue)
                    .average()
                    .orElse(0);
            double stdDev = Math.sqrt(
                    anomalyScoresMap.get(key).stream()
                            .mapToDouble(s -> Math.pow(s - mean, 2))
                            .average()
                            .orElse(0));
            System.out.println(mean);
            System.out.println(stdDev);
            dynamicThreshold = mean + 2 * stdDev;
        }
        return dynamicThreshold;
    }

    private static boolean filterAnomalies(String key, JsonObject result) {
        double score = result.get("score").getAsDouble();
        double dynamicThreshold = result.get("dynamic_threshold").getAsDouble();
        double diff = score - dynamicThreshold;
        return score > dynamicThreshold;
    }

    private static String mapToAnomalyJson(String key, JsonObject result) {
        cantAnomalies.incrementAndGet();
        JsonObject json = gson.fromJson(result, JsonObject.class);
        JsonObject anomaly = new JsonObject();
        anomaly.addProperty("date", Instant.now().toString());
        anomaly.addProperty("adj_close", json.get("adj_close").getAsDouble());
        anomaly.addProperty("method", "Isolation Forest");
        anomaly.addProperty("detected_time", Instant.now().toString());
        return anomaly.toString();
    }
    private static void logMetrics(String key,String value) {
        long currentRecords = recordCount.get();
        long currentBytes = byteCount.get();
        long currentAnomalies = cantAnomalies.get();

        Duration elapsed = Duration.between(startTime, Instant.now());
        double secondsElapsed = elapsed.toMillis() / 1000.0;

        System.out.printf("Records: %d | Bytes: %d | Anomalies: %d | Elapsed Time: %.2f sec | Records/sec: %.2f | Bytes/sec: %.2f%n",
                currentRecords, currentBytes, currentAnomalies, secondsElapsed,
                currentRecords / secondsElapsed, currentBytes / secondsElapsed);
    }

    private static CountDownLatch setupShutdownHook(KafkaStreams streams) {
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                System.out.println("Shutdown hook triggered");
                long currentRecords = recordCount.get();
                long currentBytes = byteCount.get();
                System.out.printf("Start Time: %s | Records: %d | Bytes: %d%n",
                        startTime, currentRecords, currentBytes);
                streams.close();
                latch.countDown();
            }
        });
        return latch;
    }
}
