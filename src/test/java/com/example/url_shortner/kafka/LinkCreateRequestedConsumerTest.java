package com.example.url_shortner.kafka;

import com.example.url_shortner.exceptions.DuplicateCodeException;
import com.example.url_shortner.exceptions.InvalidPayloadException;
import com.example.url_shortner.exceptions.SuffixGenerationException;
import com.example.url_shortner.models.LinkDocument;
import com.example.url_shortner.repositories.LinkRepository;
import com.example.url_shortner.service.LinkCache;
import com.example.url_shortner.utils.Constants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.net.URI;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class  LinkCreateRequestedConsumerTest {

    private static final URI TARGET_URL = URI.create("https://example.com/target");

    private LinkRepository linkRepository;
    private LinkCache linkCache;
    private Acknowledgment acknowledgment;
    private LinkCreateRequestedConsumer consumer;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        linkRepository = mock(LinkRepository.class);
        linkCache = mock(LinkCache.class);
        acknowledgment = mock(Acknowledgment.class);
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        consumer = new LinkCreateRequestedConsumer(linkRepository, linkCache, objectMapper);
    }

    @Test
    void savesCustomCodeWithTtlAndAcknowledgesAfterSaving() throws Exception {
        when(linkRepository.findByShortCode("custom1")).thenReturn(Optional.empty());

        consumer.consume(record("custom1", event("custom1", 60L)), acknowledgment);

        var savedDocument = org.mockito.ArgumentCaptor.forClass(LinkDocument.class);
        verify(linkRepository).save(savedDocument.capture());
        var persistenceOrder = inOrder(linkRepository, linkCache);
        persistenceOrder.verify(linkRepository).save(any(LinkDocument.class));
        persistenceOrder.verify(linkCache).evict("custom1");
        assertThat(savedDocument.getValue().getShortCode()).isEqualTo("custom1");
        assertThat(savedDocument.getValue().getOriginalUrl()).isEqualTo(TARGET_URL.toString());
        assertThat(savedDocument.getValue().getExpiresAt())
                .isEqualTo(savedDocument.getValue().getCreatedAt().plusSeconds(60));
        verify(linkCache).evict("custom1");
        verify(acknowledgment).acknowledge();
    }

    @Test
    void generatesCodeAndLeavesExpirationUnsetForNonPositiveTtl() throws Exception {
        when(linkRepository.existsByShortCode(any())).thenReturn(false);

        consumer.consume(record(null, event(null, 0L)), acknowledgment);

        var savedDocument = org.mockito.ArgumentCaptor.forClass(LinkDocument.class);
        verify(linkRepository).save(savedDocument.capture());
        assertThat(savedDocument.getValue().getShortCode()).matches("[a-zA-Z0-9]{7}");
        assertThat(savedDocument.getValue().getExpiresAt()).isNull();
        verify(acknowledgment).acknowledge();
    }

    @Test
    void acknowledgesMatchingExistingCustomCodeWithoutSavingAgain() throws Exception {
        var existingDocument = new LinkDocument("custom1", TARGET_URL.toString(), Instant.now(), null);
        when(linkRepository.findByShortCode("custom1")).thenReturn(Optional.of(existingDocument));

        consumer.consume(record("custom1", event("custom1", 60L)), acknowledgment);

        verify(linkRepository, never()).save(any(LinkDocument.class));
        verify(linkCache).evict("custom1");
        verify(acknowledgment).acknowledge();
    }

    @Test
    void rejectsConflictingExistingCustomCodeWithoutAcknowledging() throws Exception {
        var existingDocument = new LinkDocument(
                "custom1", "https://different.example/", Instant.now(), null);
        when(linkRepository.findByShortCode("custom1")).thenReturn(Optional.of(existingDocument));

        assertThatThrownBy(() -> consumer.consume(
                record("custom1", event("custom1", 60L)), acknowledgment))
                .isInstanceOf(DuplicateCodeException.class);

        verify(linkRepository, never()).save(any(LinkDocument.class));
        verifyNoInteractions(acknowledgment);
        verifyNoInteractions(linkCache);
    }

    @Test
    void routesMalformedJsonToInvalidPayloadFailureWithoutAcknowledging() {
        assertThatThrownBy(() -> consumer.consume(
                new ConsumerRecord<>("link-create-requests", 0, 1L, "key", "{invalid"),
                acknowledgment))
                .isInstanceOf(InvalidPayloadException.class);

        verifyNoInteractions(linkRepository, acknowledgment);
    }

    @Test
    void rejectsPayloadMissingRequiredFieldsWithoutAcknowledging() throws Exception {
        var eventMissingTtl = new LinkCreateRequested(
                UUID.randomUUID(),
                LinkCreateRequested.CURRENT_SCHEMA_VERSION,
                "custom1",
                TARGET_URL,
                null,
                Instant.now());

        assertThatThrownBy(() -> consumer.consume(
                record("custom1", eventMissingTtl), acknowledgment))
                .isInstanceOf(InvalidPayloadException.class);

        verifyNoInteractions(linkRepository, acknowledgment);
    }

    @Test
    void rejectsNullRecordValueWithoutAcknowledging() {
        assertThatThrownBy(() -> consumer.consume(
                new ConsumerRecord<>("link-create-requests", 0, 1L, "key", null),
                acknowledgment))
                .isInstanceOf(InvalidPayloadException.class);

        verifyNoInteractions(linkRepository, acknowledgment);
    }

    @Test
    void failsAfterExhaustingGeneratedCodeRetriesWithoutAcknowledging() throws Exception {
        when(linkRepository.existsByShortCode(any())).thenReturn(true);

        assertThatThrownBy(() -> consumer.consume(
                record(null, event(null, 60L)), acknowledgment))
                .isInstanceOf(SuffixGenerationException.class);

        verify(linkRepository, times(Constants.MAX_RETRIES)).existsByShortCode(any());
        verify(linkRepository, never()).save(any(LinkDocument.class));
        verifyNoInteractions(acknowledgment);
        verifyNoInteractions(linkCache);
    }

    @Test
    void doesNotAcknowledgeWhenPersistenceFails() throws Exception {
        when(linkRepository.findByShortCode("custom1")).thenReturn(Optional.empty());
        when(linkRepository.save(any(LinkDocument.class)))
                .thenThrow(new IllegalStateException("database unavailable"));

        assertThatThrownBy(() -> consumer.consume(
                record("custom1", event("custom1", 60L)), acknowledgment))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("database unavailable");

        verifyNoInteractions(acknowledgment);
    }

    private ConsumerRecord<String, String> record(String key, LinkCreateRequested event) throws Exception {
        return new ConsumerRecord<>(
                "link-create-requests", 0, 1L, key, objectMapper.writeValueAsString(event));
    }

    private LinkCreateRequested event(String customCode, Long ttlSeconds) {
        return LinkCreateRequested.of(TARGET_URL, customCode, ttlSeconds);
    }
}
