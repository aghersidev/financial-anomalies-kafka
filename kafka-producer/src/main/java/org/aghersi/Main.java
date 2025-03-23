package org.aghersi;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Main {

    private static final String SERVICE_ACCOUNT_KEY_FILE = "src/main/resources/drivekafkaproducer-7633fc40fb1d.json";
    private static final String FILE_ID = "1rMtEZocRy9gx9Afhc9FIYgEzanMv_4e9";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String APPLICATION_NAME = "DriveKafkaProducer";
    private static final String KAFKA_TOPIC = "input-data";
    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        CountDownLatch latch = new CountDownLatch(1);
        try {
            Drive driveService = getDriveService();
            KafkaProducer<String, String> producer = createKafkaProducer();

            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    System.out.println("Shutdown hook triggered");
                    producer.close();
                    latch.countDown();
                }
            });

            try (InputStream inputStream = driveService.files().get(FILE_ID).executeMediaAsInputStream();
                 BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

                reader.lines().skip(1).forEach(line -> processAndSend(line, producer));
            }
            producer.flush();
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Exiting");
        System.exit(0);
    }

    private static Drive getDriveService() throws IOException, GeneralSecurityException {
        JsonFactory json_factory = GsonFactory.getDefaultInstance();
        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        try (InputStream credentialsStream = Files.newInputStream(Paths.get(SERVICE_ACCOUNT_KEY_FILE))) {
            GoogleCredential credential = GoogleCredential.fromStream(credentialsStream)
                    .createScoped(Collections.singletonList(DriveScopes.DRIVE_READONLY));
            return new Drive.Builder(httpTransport, json_factory, credential)
                    .setApplicationName(APPLICATION_NAME)
                    .build();
        }
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass().getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass().getName());
        return new KafkaProducer<>(props);
    }

    private static void processAndSend(String line, KafkaProducer<String, String> producer) {
        String[] parts = line.split(",");
        if (parts.length < 8) return;

        try {
            Map<String, Object> message = new HashMap<>();
            message.put("date", parts[0]);
            message.put("open", parseDouble(parts[1]));
            message.put("high", parseDouble(parts[2]));
            message.put("low", parseDouble(parts[3]));
            message.put("close", parseDouble(parts[4]));
            message.put("adj_close", parseDouble(parts[5]));
            message.put("volume", parseDouble(parts[6]));

            String jsonMessage = gson.toJson(message);
            producer.send(new ProducerRecord<>(KAFKA_TOPIC, parts[7], jsonMessage));

        } catch (Exception e) {
            System.err.println("Skipping line: " + line);
        }
    }

    private static Double parseDouble(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
