package com.example.url_shortner.kafka;


import com.example.url_shortner.exceptions.DuplicateCodeException;
import com.example.url_shortner.models.LinkDocument;
import com.example.url_shortner.repositories.LinkRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.time.Instant;
import java.util.Optional;

import static com.example.url_shortner.utils.Constants.MAX_RETRIES;
import static com.example.url_shortner.utils.Utils.generateUniqueRandomCode;

@Service
public class LinkCreateRequestedConsumer {

    private static final Logger log = LoggerFactory.getLogger(LinkCreateRequestedConsumer.class);
    private final LinkRepository linkRepository;
    private final ObjectMapper objectMapper;

    public LinkCreateRequestedConsumer(LinkRepository linkRepository, ObjectMapper objectMapper) {
        this.linkRepository = linkRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "link-create-requests",
            groupId = "url-shortener-consumer-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) throws JsonProcessingException {
        String messageKey = record.key();
        LinkCreateRequested event = objectMapper.readValue(record.value(), LinkCreateRequested.class);

        if (event == null) {
            log.warn("Received empty or corrupt payload on key: {}", messageKey);
            return;
        }

        log.info("Processing LinkCreateRequested event. Key (ShortCode): {}, Event ID: {}, Schema Version: {}",
                messageKey, event.eventId(), event.schemaVersion());

        // Basic validation of payload
        if (event.targetUrl() == null || event.eventId() == null || event.ttlSeconds() == null) {
            log.error("Discarding invalid payload for event: {}", event);
            return;
        }

        String customCode = event.customCode();
        URI targetUrl = event.targetUrl();
        String code = null;

        // 1. Fetch any existing record using shortCode
        if (customCode != null) {
            Optional<LinkDocument> byShortCode = linkRepository.findByShortCode(customCode);
            if (byShortCode.isPresent()) {
                log.info("Link already exists for custom code: {}", customCode);
                throw new DuplicateCodeException("Link already exists for custom code: " + customCode);
            }
            code = customCode;
        } else {

            for (int i = 1; i <= MAX_RETRIES; i++) {
                String candidate = generateUniqueRandomCode();
                if (!linkRepository.existsByShortCode(candidate)) {
                    code = candidate;
                    break;
                }
            }
        }
        // 3. Persist the link to MongoDB
        try {
            Instant now = Instant.now();
            Instant expiresAt = event.ttlSeconds() != null
                    ? now.plusSeconds(event.ttlSeconds())
                    : null;

            var document = new LinkDocument(code, event.targetUrl().toString(), now, expiresAt);
            linkRepository.save(document);
            // Mark message complete only after successful write
            ack.acknowledge();
            log.info("Successfully persisted short link '{}' -> '{}' to MongoDB", code, targetUrl);
        } catch (Exception e) {
            log.error("Failed to save short link to MongoDB. Event: {}", event.eventId(), e);
            // Throw exception if you want Spring Kafka's error handler to trigger retries/DLQ
            throw e;
        }
    }

//        CreateLinkRequest request = new CreateLinkRequest();
//        request.setTargetUrl(event.targetUrl());
//        request.setCustomCode(event.customCode());
//        request.setTtlSeconds(event.ttlSeconds());
//        log.info("Created LinkCreateRequested event: {}", request);
//        var res = this.linkService.createLink(request);
//        log.info("LinkResponse event: {}", res);


}