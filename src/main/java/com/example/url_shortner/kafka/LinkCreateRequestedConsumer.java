package com.example.url_shortner.kafka;

import com.example.url_shortner.exceptions.DuplicateCodeException;
import com.example.url_shortner.exceptions.InvalidPayloadException;
import com.example.url_shortner.exceptions.SuffixGenerationException;
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
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) throws Exception {
        String messageKey = record.key();
        LinkCreateRequested event;

        // 1. Safe JSON Deserialization (Throw to let Spring's DLT handler deal with Poison Pills)
        try {
            if (record.value() == null) {
                throw new InvalidPayloadException("Received record with a null value/payload on key: " + messageKey);
            }
            event = objectMapper.readValue(record.value(), LinkCreateRequested.class);
        } catch (JsonProcessingException e) {
            log.error("Deserialization failed for record on partition {} offset {}. Key: {}.",
                    record.partition(), record.offset(), messageKey);
            // Throwing directly forces Spring Kafka's DefaultErrorHandler to send this straight to the DLT
            throw new InvalidPayloadException("Malformed JSON payload in partition " + record.partition() + " offset " + record.offset());
        }

        if (event == null) {
            throw new InvalidPayloadException("Deserialized payload resulted in null for key: " + messageKey);
        }

        log.info("Processing LinkCreateRequested. Partition: {}, Offset: {}, Key: {}, Event ID: {}",
                record.partition(), record.offset(), messageKey, event.eventId());

        // 2. Strict Payload Validation (Throwing instead of returning silently)
        if (event.eventId() == null || event.targetUrl() == null || event.ttlSeconds() == null) {
            log.error("Discarding invalid event payload. Missing mandatory fields. Event: {}", event);
            throw new InvalidPayloadException("Missing mandatory fields (eventId, targetUrl, or ttlSeconds) for Event ID: " + event.eventId());
        }

        String customCode = event.customCode();
        String targetUrlStr = event.targetUrl().toString();
        String code = null;

        try {
            // 3. Handle Custom Code Logic (Explicit Idempotency & Conflict Check)
            if (customCode != null) {
                if (messageKey != null && !messageKey.equals(customCode)) {
                    log.warn("Mismatched identifiers! Kafka Key ({}) does not match Payload customCode ({}).",
                            messageKey, customCode);
                }

                Optional<LinkDocument> existingDocOpt = linkRepository.findByShortCode(customCode);

                if (existingDocOpt.isPresent()) {
                    LinkDocument existingDoc = existingDocOpt.get();

                    // Case A: Idempotent Retry (Already Handled - Safe Success)
                    if (existingDoc.getOriginalUrl().equals(targetUrlStr)) {
                        log.info("Idempotent processing: Link already exists for custom code '{}' with matching target URL. " +
                                "Acknowledging message and completing turn.", customCode);
                        ack.acknowledge(); // Safely commit offset as this is already completed
                        return;
                    }
                    // Case B: Genuine Conflict (Unsafe collision)
                    else {
                        log.error("CRITICAL CONFLICT: Custom code '{}' is already claimed by a different target URL! " +
                                "Existing: '{}' | Requested: '{}'", customCode, existingDoc.getOriginalUrl(), targetUrlStr);
                        throw new DuplicateCodeException("Conflict: Custom code '" + customCode +
                                "' is already assigned to a different URL.");
                    }
                }
                code = customCode;
            }
            // 4. Handle Auto-Generated Suffix Logic
            else {
                for (int i = 1; i <= MAX_RETRIES; i++) {
                    String candidate = generateUniqueRandomCode();
                    if (!linkRepository.existsByShortCode(candidate)) {
                        code = candidate;
                        break;
                    }
                    log.warn("Collision warning: Random code candidate '{}' already exists (Attempt {}/{})", candidate, i, MAX_RETRIES);
                }

                if (code == null) {
                    log.error("Exhausted MAX_RETRIES ({}) attempting to find a unique random code.", MAX_RETRIES);
                    throw new SuffixGenerationException("Failed to generate unique random code after " + MAX_RETRIES + " attempts.");
                }
            }

            // 5. Persist to Database
            Instant now = Instant.now();
            Instant expiresAt = event.ttlSeconds() > 0 ? now.plusSeconds(event.ttlSeconds()) : null;

            var document = new LinkDocument(code, targetUrlStr, now, expiresAt);
            linkRepository.save(document);

            // 6. Acknowledge ONLY after successful completion of DB write
            ack.acknowledge();
            log.info("Successfully shortened URL. Key: {} | Code: '{}' -> '{}'. DB Save Confirmed.",
                    messageKey, code, targetUrlStr);

        } catch (DuplicateCodeException | SuffixGenerationException e) {
            // Functional failures: Propagate directly to Spring Kafka so they can be routed to the DLT instantly
            log.error("Functional processing error for Event ID: {}. Routing to DLT.", event.eventId(), e);
            throw e;
        } catch (Exception e) {
            // Transient/System failures (e.g., MongoDB timeout): Propagate to trigger Kafka retry loop
            log.error("Database connection or system error persisting short link. Event ID: {}", event.eventId(), e);
            throw e;
        }
    }
}