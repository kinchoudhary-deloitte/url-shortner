package com.example.url_shortner.service;



import com.example.demo.model.CreateLinkRequest;
import com.example.demo.model.LinkResponse;
import com.example.url_shortner.exceptions.LogCodes;
import org.openapitools.jackson.nullable.JsonNullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.security.SecureRandom;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class InMemoryLinkServiceImpl implements LinkService {

    private static final Logger log = LoggerFactory.getLogger(InMemoryLinkServiceImpl.class);
    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static final SecureRandom RANDOM = new SecureRandom();

    private final String baseUrl;
    private final Map<String, LinkRecord> store = new ConcurrentHashMap<>();

    public InMemoryLinkServiceImpl(@Value("${app.base-url:http://localhost:8080}") String baseUrl) {
        this.baseUrl = baseUrl;
    }

    private record LinkRecord(String code, String targetUrl, OffsetDateTime createdAt, OffsetDateTime expiresAt, AtomicLong clicks) {}

    @Override
    public LinkResponse createLink(CreateLinkRequest request) {
        String requestId = MDC.get("requestId");
        String code = (request.getCustomCode() != null && !request.getCustomCode().isBlank())
                ? request.getCustomCode()
                : generateRandomCode(7);

        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime expiresAt = request.getTtlSeconds() != null
                ? now.plusSeconds(request.getTtlSeconds())
                : null;

        var record = new LinkRecord(code, request.getTargetUrl().toString(), now, expiresAt, new AtomicLong(0));

        try {
            store.put(code, record);
            // Safe logging: do not log full target URL if it contains sensitive tokens
            log.info("event=link_created requestId={} shortCode={} hasExpiry={}",
                    requestId, code, expiresAt != null);
        } catch (Exception ex) {
            log.error("code={} event=link_persistence_failed requestId={} exception={}",
                    LogCodes.ERR_LINK_PERSISTENCE, requestId, ex.getClass().getSimpleName(), ex);
            throw ex;
        }

        return toResponse(record);
    }

    @Override
    public Optional<LinkResponse> findByCode(String code) {
        return Optional.ofNullable(store.get(code)).map(this::toResponse);
    }

    @Override
    public Optional<String> resolveAndRecordClick(String code) {
        LinkRecord record = store.get(code);
        if (record == null) {
            return Optional.empty();
        }
        if (record.expiresAt() != null && record.expiresAt().isBefore(OffsetDateTime.now())) {
            return Optional.empty();
        }
        record.clicks().incrementAndGet();
        return Optional.of(record.targetUrl());
    }

    private LinkResponse toResponse(LinkRecord r) {
        var resp = new LinkResponse();
        resp.setCode(r.code());
        resp.setTargetUrl(java.net.URI.create(r.targetUrl()));
        resp.setShortUrl(java.net.URI.create(baseUrl + "/" + r.code()));
        resp.setCreatedAt(r.createdAt());
        resp.setExpiresAt(r.expiresAt()!= null ? JsonNullable.of(r.expiresAt()) : JsonNullable.undefined() );
        resp.setClickCount(r.clicks().get());
        return resp;
    }

    private String generateRandomCode(int length) {
        var sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(ALPHABET.charAt(RANDOM.nextInt(ALPHABET.length())));
        }
        return sb.toString();
    }
}