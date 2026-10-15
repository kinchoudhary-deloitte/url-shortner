package com.example.url_shortner.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class NativeKafkaCliProducer {
    private static final Logger log = LoggerFactory.getLogger(NativeKafkaCliProducer.class);

    private static final String TOPIC = "link-create-requests";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    // Sample URLs to randomly pick from
    private static final List<String> SAMPLE_URLS = List.of(
            "https://docs.spring.io",
            "https://github.com/spring-projects/spring-kafka",
            "https://kafka.apache.org/documentation",
            "https://stackoverflow.com",
            "https://medium.com"
    );

    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

        // Initialize the producer (keep it open for the duration of the app)
        Producer<String, String> producer = new KafkaProducer<>(props);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        // Register a JVM shutdown hook to clean up resources gracefully on Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received. Cleaning up resources...");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
            }
            producer.close();
            log.info("Producer successfully closed.");
        }));

        log.info("Scheduler started. Generating a random link request every 3 minutes...");

        // Schedule the task: initial delay of 0, repeating every 3 minutes
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // 1. Generate randomized key values matching the new fields
                String customCode = "lnk" + UUID.randomUUID().toString().substring(0, 8);
                String randomStringUrl = SAMPLE_URLS.get(RANDOM.nextInt(SAMPLE_URLS.size()));

// 2. Safely parse the String URL into a java.net.URI
                URI targetUrl = URI.create(randomStringUrl);

// 3. Define a random TTL timestamp between 1 and 5 minutes from now
//                long ttlSeconds = (1 + RANDOM.nextInt(4))*60;

                long ttlSeconds = 60;
// 4. Construct the updated event record payload
                LinkCreateRequested event = LinkCreateRequested.of(targetUrl,customCode,ttlSeconds);
                String payloadJson = mapper.writeValueAsString(event);

// 5. Build the ProducerRecord using the customCode as the message routing key
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, event.customCode(), payloadJson);

// 6. Send asynchronously to avoid blocking the scheduler thread
                producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                    if (exception != null) {
                        log.error("Failed to send message: {}", exception.getMessage(), exception);
                    } else {
                        log.info("Published random event [Code: {}, Target URI: {}, TTL: {}] to partition {} at offset {}",
                                customCode, targetUrl, ttlSeconds, metadata.partition(), metadata.offset());
                    }
                });

            } catch (Exception e) {
                log.error("Error occurred while generating/sending scheduled event", e);
            }
        }, 0, 1, TimeUnit.MINUTES);
    }
}
