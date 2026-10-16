package com.example.url_shortner.services;

import com.example.url_shortner.models.LinkDocument;
import com.example.url_shortner.repositories.LinkRepository;
import com.example.url_shortner.service.LinkCache;
import com.example.url_shortner.service.MongoLinkServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LinkCacheTest {

    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> valueOperations = mock(ValueOperations.class);

    private final StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);

    @Test
    void readsAndWritesRedisValuesWithExpiry() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("url-shortner:link:abc")).thenReturn("https://example.com");
        LinkCache cache = new LinkCache(redisTemplate);

        assertThat(cache.get("abc")).contains("https://example.com");
        cache.put("abc", "https://example.com", Duration.ofMinutes(5));

        verify(valueOperations).set(
                "url-shortner:link:abc", "https://example.com", Duration.ofMinutes(5));
    }

    @Test
    void treatsRedisReadFailureAsCacheMissAndResolvesFromMongo() {
        when(redisTemplate.opsForValue())
                .thenThrow(new DataAccessResourceFailureException("Redis unavailable"));

        LinkRepository repository = mock(LinkRepository.class);
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        var document = new LinkDocument("abc", "https://example.com", Instant.now(), null);
        when(repository.findByShortCode("abc")).thenReturn(Optional.of(document));

        var service = new MongoLinkServiceImpl(
                repository, mongoTemplate, new LinkCache(redisTemplate),
                "http://localhost:8080", Duration.ofMinutes(5));

        assertThat(service.resolveAndRecordClick("abc")).contains("https://example.com");
        verify(repository).findByShortCode("abc");
    }

    @Test
    void ignoresRedisWriteFailureAfterMongoLookup() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("url-shortner:link:abc")).thenReturn(null);
        doThrow(new DataAccessResourceFailureException("Redis unavailable"))
                .when(valueOperations)
                .set(eq("url-shortner:link:abc"), eq("https://example.com"), eq(Duration.ofMinutes(5)));
        LinkRepository repository = mock(LinkRepository.class);
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        var document = new LinkDocument("abc", "https://example.com", Instant.now(), null);
        when(repository.findByShortCode("abc")).thenReturn(Optional.of(document));

        var service = new MongoLinkServiceImpl(
                repository, mongoTemplate, new LinkCache(redisTemplate),
                "http://localhost:8080", Duration.ofMinutes(5));

        assertThat(service.resolveAndRecordClick("abc")).contains("https://example.com");
    }
}
