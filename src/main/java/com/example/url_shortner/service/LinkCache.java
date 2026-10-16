package com.example.url_shortner.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;

@Component
public class LinkCache {

    private static final Logger log = LoggerFactory.getLogger(LinkCache.class);
    private static final String KEY_PREFIX = "url-shortner:link:";

    private final StringRedisTemplate redisTemplate;

    public LinkCache(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public Optional<String> get(String code) {
        try {
            String targetUrl = redisTemplate.opsForValue().get(key(code));
            String requestId = MDC.get("requestId");
            if (targetUrl == null) {
                log.info("level=INFO event=link_cache_miss requestId={} shortCode={}", requestId, code);
                return Optional.empty();
            }

            log.info("level=INFO event=link_cache_hit requestId={} shortCode={}", requestId, code);
            return Optional.of(targetUrl);
        } catch (DataAccessException ex) {
            log.warn("level=WARN event=link_cache_read_failed requestId={} shortCode={} exception={}; falling back to MongoDB",
                    MDC.get("requestId"), code, ex.getClass().getSimpleName(), ex);
            return Optional.empty();
        }
    }

    public void put(String code, String targetUrl, Duration ttl) {
        if (ttl.isZero() || ttl.isNegative()) {
            return;
        }

        try {
            redisTemplate.opsForValue().set(key(code), targetUrl, ttl);
            log.info("level=INFO event=link_cache_populated requestId={} shortCode={} ttlSeconds={}",
                    MDC.get("requestId"), code, ttl.toSeconds());
        } catch (DataAccessException ex) {
            log.warn("level=WARN event=link_cache_write_failed requestId={} shortCode={} exception={}",
                    MDC.get("requestId"), code, ex.getClass().getSimpleName(), ex);
        }
    }

    public void evict(String code) {
        try {
            Boolean evicted = redisTemplate.delete(key(code));
            log.info("level=INFO event=link_cache_evicted requestId={} shortCode={} entryExisted={}",
                    MDC.get("requestId"), code, Boolean.TRUE.equals(evicted));
        } catch (DataAccessException ex) {
            log.warn("level=WARN event=link_cache_eviction_failed requestId={} shortCode={} exception={}",
                    MDC.get("requestId"), code, ex.getClass().getSimpleName(), ex);
        }
    }

    private String key(String code) {
        return KEY_PREFIX + code;
    }
}
