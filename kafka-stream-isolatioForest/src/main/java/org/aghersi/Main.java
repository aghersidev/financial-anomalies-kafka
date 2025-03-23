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
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Main {
    private static final Gson gson = new Gson();
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String KAFKA_GROUP = "anomaly-tree-detector";
    private static final String KAFKA_TOPIC = "augmented";
    private static final String KAFKA_OUTPUT_TOPIC = "anomalies";
    private static final int ROLLING_WINDOW_SIZE = 100;
    private static final Map<String, List<double[]>> featureVectorsMap = new HashMap<>();
    private static final Map<String, IsolationForest> isolationForestMap = new HashMap<>();
    private static final Map<String, List<Double>> anomalyScoresMap = new HashMap<>();

    public static void main(String[] args) {
        Properties props = createStreamsConfig();
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(KAFKA_TOPIC);

        stream
                .map(Main::mapToAnomalyScore)
                .filter(Main::filterAnomalies)
                .mapValues(Main::mapToAnomalyJson)
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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KAFKA_GROUP + Instant.now());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    private static KeyValue<String, JsonObject> mapToAnomalyScore(String key, String value) {
        JsonObject json = gson.fromJson(value, JsonObject.class);
        double adjClose = json.get("adj_close").getAsDouble();
        double volume = json.get("volume").getAsDouble();
        double up = json.get("up").getAsDouble();
        double down = json.get("down").getAsDouble();
        double size = json.get("size").getAsDouble();
        double[] features = {adjClose, volume, up, down, size};

        featureVectorsMap.putIfAbsent(key, new ArrayList<>());
        anomalyScoresMap.putIfAbsent(key, new ArrayList<>());
        isolationForestMap.putIfAbsent(key, new IsolationForest(100, 4));

        featureVectorsMap.get(key).add(features);

        if (featureVectorsMap.get(key).size() > ROLLING_WINDOW_SIZE) {
            featureVectorsMap.get(key).remove(0);
        }

        IsolationForest trainedForest = isolationForestMap.get(key).fit(featureVectorsMap.get(key).toArray(new double[0][]));
        isolationForestMap.put(key, trainedForest);

        double score = isolationForestMap.get(key).score(features);
        anomalyScoresMap.get(key).add(score);

        if (anomalyScoresMap.get(key).size() > ROLLING_WINDOW_SIZE) {
            anomalyScoresMap.get(key).remove(0);
        }

        double dynamicThreshold = calculateDynamicThreshold(key);

        JsonObject result = new JsonObject();
        result.addProperty("score", score);
        result.addProperty("adj_close", adjClose);
        result.addProperty("dynamic_threshold", dynamicThreshold);

        return new KeyValue<>(key, result);
    }

    private static double calculateDynamicThreshold(String key) {
        double dynamicThreshold = 0;
        if (anomalyScoresMap.get(key).size() >= 2) {
            double mean = anomalyScoresMap.get(key).stream().mapToDouble(Double::doubleValue).average().orElse(0);
            double stdDev = Math.sqrt(anomalyScoresMap.get(key).stream().mapToDouble(s -> Math.pow(s - mean, 2)).average().orElse(0));
            dynamicThreshold = mean + 2 * stdDev;
        }
        return dynamicThreshold;
    }

    private static boolean filterAnomalies(String key, JsonObject result) {
        double score = result.get("score").getAsDouble();
        double dynamicThreshold = result.get("dynamic_threshold").getAsDouble();
        return score > dynamicThreshold;
    }

    private static String mapToAnomalyJson(String key, JsonObject result) {
        JsonObject json = gson.fromJson(result, JsonObject.class);
        JsonObject anomaly = new JsonObject();
        anomaly.addProperty("date", Instant.now().toString());
        anomaly.addProperty("adj_close", json.get("adj_close").getAsDouble());
        anomaly.addProperty("method", "Isolation Forest");
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
