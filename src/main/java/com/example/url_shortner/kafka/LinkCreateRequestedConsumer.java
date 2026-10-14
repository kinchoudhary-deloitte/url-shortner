package com.example.url_shortner.kafka;


import com.example.demo.model.CreateLinkRequest;
import com.example.url_shortner.models.LinkDocument;
import com.example.url_shortner.repositories.LinkRepository;
import com.example.url_shortner.service.LinkService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class LinkCreateRequestedConsumer {

    private static final Logger log = LoggerFactory.getLogger(LinkCreateRequestedConsumer.class);
    private final LinkService linkService;
    private final ObjectMapper objectMapper;
    public LinkCreateRequestedConsumer(LinkService linkService, ObjectMapper objectMapper) {
        this.linkService = linkService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "link-create-requests",
            groupId = "url-shortener-consumer-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record) throws JsonProcessingException {
        String messageKey = record.key();
        LinkCreateRequested event = objectMapper.readValue(record.value(),LinkCreateRequested.class);

        if (event == null) {
            log.warn("Received empty or corrupt payload on key: {}", messageKey);
            return;
        }

        log.info("Processing LinkCreateRequested event. Key (ShortCode): {}, Event ID: {}, Schema Version: {}",
                messageKey, event.eventId(), event.schemaVersion());

        // Basic validation of payload
        if (event.targetUrl() == null) {
            log.error("Discarding invalid payload for event: {}", event.eventId());
            return;
        }

        CreateLinkRequest request = new CreateLinkRequest();
        request.setTargetUrl(event.targetUrl());
        request.setCustomCode(event.customCode());
        request.setTtlSeconds(event.ttlSeconds());
        log.info("Created LinkCreateRequested event: {}", request);
        var res = this.linkService.createLink(request);
        log.info("LinkResponse event: {}", res);


    }
}