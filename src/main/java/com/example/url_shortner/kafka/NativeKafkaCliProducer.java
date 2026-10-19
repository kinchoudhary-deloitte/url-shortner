package com.example.url_shortner.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

@Service
public class NativeKafkaCliProducer {
    private static final Logger log = LoggerFactory.getLogger(NativeKafkaCliProducer.class);
    private static final String TOPIC = "link-create-requests";

    private final Producer<String, String> producer;
    private final ObjectMapper objectMapper;

    public NativeKafkaCliProducer(Producer<String, String> producer, ObjectMapper objectMapper) {
        this.producer = producer;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<LinkCreateRequested> publish(URI targetUrl, String customCode, Long ttlSeconds) {
        LinkCreateRequested event = LinkCreateRequested.of(targetUrl, customCode, ttlSeconds);
        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize link creation event", e);
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, event.customCode(), payload);
        CompletableFuture<LinkCreateRequested> result = new CompletableFuture<>();

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to publish link creation event {}", event.eventId(), exception);
                result.completeExceptionally(exception);
                return;
            }

            log.info("Published link creation event {} to partition {} at offset {}",
                    event.eventId(), metadata.partition(), metadata.offset());
            result.complete(event);
        });

        return result;
    }
}
