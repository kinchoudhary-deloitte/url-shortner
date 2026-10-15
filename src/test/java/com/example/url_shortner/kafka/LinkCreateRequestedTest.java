package com.example.url_shortner.kafka;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class LinkCreateRequestedTest {

    @Test
    void factoryCreatesEventWithCurrentSchemaAndGeneratedMetadata() {
        Instant beforeCreation = Instant.now();

        LinkCreateRequested event = LinkCreateRequested.of(
                URI.create("https://example.com/target"), "custom1", 120L);

        assertThat(event.eventId()).isNotNull().isInstanceOf(UUID.class);
        assertThat(event.schemaVersion()).isEqualTo(LinkCreateRequested.CURRENT_SCHEMA_VERSION);
        assertThat(event.customCode()).isEqualTo("custom1");
        assertThat(event.targetUrl()).isEqualTo(URI.create("https://example.com/target"));
        assertThat(event.ttlSeconds()).isEqualTo(120L);
        assertThat(event.createdAt()).isAfterOrEqualTo(beforeCreation);
        assertThat(event.createdAt()).isBeforeOrEqualTo(Instant.now());
    }
}
