package com.example.url_shortner.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NativeKafkaCliProducerTest {

    private static final URI TARGET_URL = URI.create("https://example.com/target");

    private Producer<String, String> kafkaProducer;
    private NativeKafkaCliProducer producer;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        kafkaProducer = mock(Producer.class);
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        producer = new NativeKafkaCliProducer(kafkaProducer, objectMapper);
    }

    @Test
    void publishesJsonEventUsingCustomCodeAsKafkaKey() throws Exception {
        RecordMetadata metadata = mock(RecordMetadata.class);
        when(metadata.partition()).thenReturn(0);
        when(metadata.offset()).thenReturn(12L);
        AtomicReference<ProducerRecord<String, String>> sentRecord = new AtomicReference<>();
        doAnswer(invocation -> {
            sentRecord.set(invocation.getArgument(0));
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(metadata, null);
            return null;
        }).when(kafkaProducer).send(any(), any(Callback.class));

        LinkCreateRequested event = producer.publish(TARGET_URL, "custom1", 60L).join();

        verify(kafkaProducer).send(any(), any(Callback.class));
        ProducerRecord<String, String> record = sentRecord.get();
        LinkCreateRequested payload = objectMapper.readValue(record.value(), LinkCreateRequested.class);

        assertThat(record.topic()).isEqualTo("link-create-requests");
        assertThat(record.key()).isEqualTo("custom1");
        assertThat(payload).isEqualTo(event);
        assertThat(event.targetUrl()).isEqualTo(TARGET_URL);
        assertThat(event.ttlSeconds()).isEqualTo(60L);
    }

    @Test
    void completesExceptionallyWhenKafkaAcknowledgesWithFailure() {
        RuntimeException kafkaFailure = new RuntimeException("broker unavailable");
        doAnswer(invocation -> {
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(null, kafkaFailure);
            return null;
        }).when(kafkaProducer).send(any(), any(Callback.class));

        var result = producer.publish(TARGET_URL, null, 60L);

        assertThatThrownBy(result::join)
                .isInstanceOf(CompletionException.class)
                .hasCause(kafkaFailure);
    }
}
