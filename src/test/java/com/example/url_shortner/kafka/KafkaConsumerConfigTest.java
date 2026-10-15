package com.example.url_shortner.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class KafkaConsumerConfigTest {

    @Test
    void consumerFactoryDisablesAutoCommitAndConsumesRawStrings() {
        var config = new KafkaConsumerConfig(mockKafkaTemplate());

        var consumerFactory = (DefaultKafkaConsumerFactory<String, String>) config.consumerFactory();

        assertThat(consumerFactory.getConfigurationProperties())
                .containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                .containsEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .containsEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                .containsEntry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @Test
    void listenerContainerUsesManualImmediateAcknowledgmentAndErrorHandler() {
        var config = new KafkaConsumerConfig(mockKafkaTemplate());
        var consumerFactory = config.consumerFactory();

        ConcurrentKafkaListenerContainerFactory<String, String> listenerFactory =
                config.kafkaListenerContainerFactory(consumerFactory);

        assertThat(listenerFactory.getContainerProperties().getAckMode())
                .isEqualTo(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    }

    private KafkaTemplate<Object, Object> mockKafkaTemplate() {
        return mock(KafkaTemplate.class);
    }
}
