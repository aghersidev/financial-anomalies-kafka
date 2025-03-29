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

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class Main {
    private static final Gson gson = new Gson();
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String KAFKA_GROUP = "anomaly-tree-detector46";
    private static final String KAFKA_TOPIC = "augmented";
    private static final String KAFKA_OUTPUT_TOPIC = "anomalies";
    private static final int ROLLING_WINDOW_SIZE = 100;
    private static final Map<String, List<double[]>> featureVectorsMap = new HashMap<>();
    private static final Map<String, IsolationForest> isolationForestMap = new LinkedHashMap<>(256, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, IsolationForest> eldest) {
            return size() > 200;
        }
    };
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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KAFKA_GROUP);
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
        keyFeaturesList.add(features);

        if (keyFeaturesList.size() > ROLLING_WINDOW_SIZE) {
            IsolationForest trainedForest = IsolationForest.fit(keyFeaturesList.toArray(new double[0][]));
            isolationForestMap.put(key, trainedForest);
            keyFeaturesList.clear();
        }
        double score = isolationForestMap.get(key).score(features);
        if (!Double.isNaN(score)) {
            List<Double> scores = anomalyScoresMap.get(key);
            scores.add(score);
            if (scores.size() > 2 * ROLLING_WINDOW_SIZE) {
                anomalyScoresMap.put(key, scores.subList(ROLLING_WINDOW_SIZE, 2 * ROLLING_WINDOW_SIZE));
            }
        }

        JsonObject result = new JsonObject();
        result.addProperty("score", score);
        result.addProperty("adj_close", adjClose);
        result.addProperty("dynamic_threshold", calculateDynamicThreshold(key));
        return new KeyValue<>(key, result);
    }

    private static double[] getFeatures(String value) {
        recordCount.incrementAndGet();
        byteCount.addAndGet(value.getBytes().length);
        JsonObject json = gson.fromJson(value, JsonObject.class);
        double adjClose = json.has("adj_close") ? json.get("adj_close").getAsDouble() : 0.0;
        double volume = json.has("volume") ? json.get("volume").getAsDouble() : 0.0;
        double up = json.has("up") ? json.get("up").getAsDouble() : 0.0;
        double down = json.has("down") ? json.get("down").getAsDouble() : 0.0;
        double size = json.has("size") ? json.get("size").getAsDouble() : 0.0;
        double[] features = {adjClose, volume, up, down, size};
        return features;
    }

    private static double calculateDynamicThreshold(String key) {
        double dynamicThreshold = 0;
        if (anomalyScoresMap.get(key).size() >= 2) {
            double mean = anomalyScoresMap.get(key).stream()
                    .mapToDouble(Double::doubleValue)
                    .average()
                    .orElse(0);
            double stdDev = Math.sqrt(
                    anomalyScoresMap.get(key).stream()
                            .mapToDouble(s -> Math.pow(s - mean, 2))
                            .average()
                            .orElse(0));
            return mean + 2 * stdDev;
        }
        return 0;
    }

    private static boolean filterAnomalies(String key, JsonObject result) {
        double score = result.get("score").getAsDouble();
        double dynamicThreshold = result.get("dynamic_threshold").getAsDouble();
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

    private static void logMetrics(String key, String value) {
        long currentRecords = recordCount.get();
        boolean val = currentRecords > 1000000 && currentRecords < 1001000;
        boolean val2 = currentRecords > 2000000 && currentRecords < 2001000;
        boolean val3 = currentRecords > 3000000 && currentRecords < 3001000;
        boolean val4 = currentRecords > 4000000 && currentRecords < 4001000;
        boolean val5 = currentRecords > 5000000 && currentRecords < 5001000;
        boolean val6 = currentRecords > 6000000 && currentRecords < 6001000;
        boolean val7 = currentRecords > 7000000 && currentRecords < 7001000;
        boolean val8 = currentRecords > 8000000 && currentRecords < 8001000;
        boolean val9 = currentRecords > 9000000 && currentRecords < 9001000;
        boolean val91 = currentRecords > 9250000 && currentRecords < 9251000;
        boolean val92 = currentRecords > 9500000 && currentRecords < 9600000;
        boolean val93 = currentRecords > 9750000 && currentRecords < 9751000;
        boolean val10 = currentRecords > 10250000 && currentRecords < 10251000;
        boolean val11 = currentRecords > 10500000 && currentRecords < 10501000;
        boolean val12 = currentRecords > 10750000 && currentRecords < 10751000;
        boolean val13 = currentRecords > 11000000;
        boolean val0 = val || val2 || val3 || val4 || val5 || val6 || val7 || val8 || val9 || val10 || val11 || val12 || val13 || val91 || val93 || val92;
        if (!val0) {
            return;
        }
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
