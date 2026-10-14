package com.example.url_shortner.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;
import java.time.Instant;
import java.util.UUID;

public record LinkCreateRequested(
        @JsonProperty("eventId") UUID eventId,
        @JsonProperty("schemaVersion") int schemaVersion,
        @JsonProperty("customCode") String customCode,
        @JsonProperty("targetUrl") URI targetUrl,
        @JsonProperty("ttlSeconds") Long ttlSeconds
) {

    public static final int CURRENT_SCHEMA_VERSION = 1;

    /**
     * Factory helper to instantiate a new event with defaults.
     */
    public static LinkCreateRequested of(URI targetUrl, String customCode, Long ttlSeconds) {
        return new LinkCreateRequested(
                UUID.randomUUID(),
                CURRENT_SCHEMA_VERSION,
                customCode, targetUrl,
                ttlSeconds
        );
    }
}