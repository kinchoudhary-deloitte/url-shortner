package com.example.url_shortner.integration;

import com.example.url_shortner.controllers.KafkaProducerController;
import com.example.url_shortner.controllers.LinkApiController;
import com.example.url_shortner.controllers.RedirectController;
import com.example.url_shortner.exceptions.GlobalExceptionHandler;
import com.example.url_shortner.kafka.LinkCreateRequested;
import com.example.url_shortner.kafka.LinkCreateRequestedConsumer;
import com.example.url_shortner.kafka.NativeKafkaCliProducer;
import com.example.url_shortner.models.LinkDocument;
import com.example.url_shortner.repositories.LinkRepository;
import com.example.url_shortner.service.LinkCache;
import com.example.url_shortner.service.MongoLinkServiceImpl;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.openapitools.jackson.nullable.JsonNullableModule;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class UrlShortnerEndToEndTest {

    private static final String TARGET_URL = "https://example.com/articles/end-to-end";

    private final Map<String, LinkDocument> persistedLinks = new HashMap<>();
    private final AtomicReference<String> cachedUrl = new AtomicReference<>();

    private LinkRepository linkRepository;
    private MongoTemplate mongoTemplate;
    private StringRedisTemplate redisTemplate;
    private ValueOperations<String, String> valueOperations;
    private Producer<String, String> kafkaProducer;
    private ObjectMapper objectMapper;
    private MockMvc mockMvc;
    private LinkCreateRequestedConsumer eventConsumer;
    private ProducerRecord<String, String> sentKafkaRecord;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        persistedLinks.clear();
        cachedUrl.set(null);
        sentKafkaRecord = null;

        linkRepository = mock(LinkRepository.class);
        mongoTemplate = mock(MongoTemplate.class);
        redisTemplate = mock(StringRedisTemplate.class);
        valueOperations = mock(ValueOperations.class);
        kafkaProducer = (Producer<String, String>) mock(Producer.class);
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .registerModule(new JsonNullableModule());

        when(linkRepository.existsByShortCode(anyString()))
                .thenAnswer(invocation -> persistedLinks.containsKey(invocation.getArgument(0)));
        when(linkRepository.findByShortCode(anyString()))
                .thenAnswer(invocation -> Optional.ofNullable(persistedLinks.get(invocation.getArgument(0))));
        when(linkRepository.save(any(LinkDocument.class))).thenAnswer(invocation -> {
            LinkDocument document = invocation.getArgument(0);
            persistedLinks.put(document.getShortCode(), document);
            return document;
        });

        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(anyString())).thenAnswer(invocation ->
                cachedUrl.get());
        doAnswer(invocation -> {
            cachedUrl.set(invocation.getArgument(1));
            return null;
        }).when(valueOperations).set(anyString(), anyString(), any(Duration.class));
        when(redisTemplate.delete(anyString())).thenAnswer(invocation -> cachedUrl.getAndSet(null) != null);

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("link-create-requests", 0),
                12L, 0, System.currentTimeMillis(), 0, 0);
        doAnswer(invocation -> {
            sentKafkaRecord = invocation.getArgument(0);
            Callback callback = invocation.getArgument(1);
            callback.onCompletion(metadata, null);
            return null;
        }).when(kafkaProducer).send(any(), any(Callback.class));

        var linkService = new MongoLinkServiceImpl(
                linkRepository, mongoTemplate, new LinkCache(redisTemplate),
                "http://localhost:8080", Duration.ofMinutes(3));
        var eventProducer = new NativeKafkaCliProducer(kafkaProducer, objectMapper);
        eventConsumer = new LinkCreateRequestedConsumer(
                linkRepository, new LinkCache(redisTemplate), objectMapper);
        mockMvc = MockMvcBuilders.standaloneSetup(
                        new LinkApiController(linkService),
                        new RedirectController(linkService),
                        new KafkaProducerController(eventProducer))
                .setControllerAdvice(new GlobalExceptionHandler())
                .setMessageConverters(new MappingJackson2HttpMessageConverter(objectMapper))
                .build();
    }

    @Test
    void createsLinkAndRedirectsThroughRedisCache() throws Exception {
        mockMvc.perform(post("/api/v1/links")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"targetUrl":"%s","customCode":"e2e-link","ttlSeconds":3600}
                                """.formatted(TARGET_URL)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.code").value("e2e-link"))
                .andExpect(jsonPath("$.shortUrl").value("http://localhost:8080/e2e-link"));

        assertThat(persistedLinks).containsKey("e2e-link");

        mockMvc.perform(get("/e2e-link"))
                .andExpect(status().isFound())
                .andExpect(header().string("Location", TARGET_URL));
        mockMvc.perform(get("/e2e-link"))
                .andExpect(status().isFound())
                .andExpect(header().string("Location", TARGET_URL));

        verify(linkRepository, times(1)).findByShortCode("e2e-link");
        verify(valueOperations, times(2)).get("url-shortner:link:e2e-link");
        verify(valueOperations).set(
                "url-shortner:link:e2e-link", TARGET_URL, Duration.ofMinutes(3));
        verify(mongoTemplate, times(2)).updateFirst(any(), any(), any(Class.class));
    }

    @Test
    void publishesKafkaMessagePersistsItAndIgnoresDuplicateDelivery() throws Exception {
        MvcResult publishResult = mockMvc.perform(post("/api/v1/events/link-create-requests")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"targetUrl":"%s","customCode":"kafka-e2e","ttlSeconds":3600}
                                """.formatted(TARGET_URL)))
                .andExpect(request().asyncStarted())
                .andReturn();

        MvcResult published = mockMvc.perform(asyncDispatch(publishResult))
                .andExpect(status().isAccepted())
                .andReturn();
        JsonNode response = objectMapper.readTree(published.getResponse().getContentAsString());

        assertThat(response.path("customCode").asText()).isEqualTo("kafka-e2e");
        assertThat(sentKafkaRecord).isNotNull();
        assertThat(sentKafkaRecord.topic()).isEqualTo("link-create-requests");
        assertThat(sentKafkaRecord.key()).isEqualTo("kafka-e2e");

        LinkCreateRequested publishedEvent =
                objectMapper.readValue(sentKafkaRecord.value(), LinkCreateRequested.class);
        assertThat(publishedEvent.targetUrl()).isEqualTo(URI.create(TARGET_URL));

        var kafkaRecord = new ConsumerRecord<>(
                sentKafkaRecord.topic(), 0, 12L, sentKafkaRecord.key(), sentKafkaRecord.value());
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        eventConsumer.consume(kafkaRecord, acknowledgment);
        eventConsumer.consume(kafkaRecord, acknowledgment);

        assertThat(persistedLinks).containsKey("kafka-e2e");
        assertThat(persistedLinks.get("kafka-e2e").getOriginalUrl()).isEqualTo(TARGET_URL);
        verify(linkRepository, times(1)).save(any(LinkDocument.class));
        verify(linkRepository, times(2)).findByShortCode("kafka-e2e");
        verify(acknowledgment, times(2)).acknowledge();
    }
}
