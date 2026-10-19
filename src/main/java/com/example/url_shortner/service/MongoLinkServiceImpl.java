package com.example.url_shortner.service;

import com.example.demo.model.CreateLinkRequest;
import com.example.demo.model.LinkResponse;
import com.example.url_shortner.exceptions.DuplicateCodeException;
import com.example.url_shortner.exceptions.InvalidUrlException;
import com.example.url_shortner.exceptions.LogCodes;
import com.example.url_shortner.models.LinkDocument;
import com.example.url_shortner.repositories.LinkRepository;
import org.openapitools.jackson.nullable.JsonNullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import static com.example.url_shortner.utils.Constants.CODE_LENGTH;
import static com.example.url_shortner.utils.Constants.MAX_RETRIES;
import static com.example.url_shortner.utils.Utils.generateUniqueRandomCode;

@Service
public class MongoLinkServiceImpl implements LinkService {

    private static final Logger log = LoggerFactory.getLogger(MongoLinkServiceImpl.class);

    private final LinkRepository linkRepository;
    private final MongoTemplate mongoTemplate;
    private final LinkCache linkCache;
    private final String baseUrl;
    private final Duration linkCacheTtl;

    public MongoLinkServiceImpl(LinkRepository linkRepository,
                                MongoTemplate mongoTemplate,
                                LinkCache linkCache,
                                @Value("${app.base-url:http://localhost:8080}") String baseUrl,
                                @Value("${app.cache.link-ttl:5m}") Duration linkCacheTtl) {
        this.linkRepository = linkRepository;
        this.mongoTemplate = mongoTemplate;
        this.linkCache = linkCache;
        this.baseUrl = baseUrl;
        this.linkCacheTtl = linkCacheTtl;
    }

    @Override
    public LinkResponse createLink(CreateLinkRequest request) {
        String requestId = MDC.get("requestId");

        // 1. Validate Target URL
        validateUrl(request.getTargetUrl());

        // 2. Select or generate short code
        String code = null;
        if (request.getCustomCode() != null) {
            code = request.getCustomCode().trim();
            if (linkRepository.existsByShortCode(code)) {
                log.warn("level=WARN code={} event=duplicate_code requestId={} shortCode={}",
                        LogCodes.WARN_DUPLICATE_CODE, requestId, code);
                throw new DuplicateCodeException("Short code '" + code + "' is already in use.");
            }
        } else {
            for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
                String candidate = generateUniqueRandomCode();
                if (!linkRepository.existsByShortCode(candidate)) {
                    code = candidate;
                    break;
                }
            }
            if (code == null) {
                throw new IllegalStateException("Failed to generate a unique short code after "
                        + MAX_RETRIES + " attempts.");
            }
        }

        Instant now = Instant.now();
        Instant expiresAt = request.getTtlSeconds() != null
                ? now.plusSeconds(request.getTtlSeconds())
                : null;

        var document = new LinkDocument(code, request.getTargetUrl().toString(), now, expiresAt);

        try {
            LinkDocument saved = linkRepository.save(document);
            log.info("level=INFO event=link_created requestId={} shortCode={} hasExpiry={}",
                    requestId, saved.getShortCode(), expiresAt != null);
            return toResponse(saved);
        } catch (DuplicateKeyException ex) {
            log.warn("level=WARN code={} event=duplicate_code requestId={} shortCode={}",
                    LogCodes.WARN_DUPLICATE_CODE, requestId, code);
            throw new DuplicateCodeException("Short code '" + code + "' is already in use.");
        } catch (Exception ex) {
            log.error("level=ERROR code={} event=link_persistence_failed requestId={} exception={}",
                    LogCodes.ERR_LINK_PERSISTENCE, requestId, ex.getClass().getSimpleName(), ex);
            throw ex;
        }
    }

    @Override
    public Optional<LinkResponse> findByCode(String code) {
        String requestId = MDC.get("requestId");
        Optional<LinkDocument> docOpt = linkRepository.findByShortCode(code);

        if (docOpt.isEmpty()) {
            return Optional.empty();
        }

        LinkDocument doc = docOpt.get();
        if (isExpired(doc)) {
            log.warn("level=WARN code={} event=link_expired requestId={} shortCode={}",
                    LogCodes.WARN_LINK_EXPIRED, requestId, code);
            return Optional.empty();
        }

        return Optional.of(toResponse(doc));
    }

    @Override
    public Optional<String> resolveAndRecordClick(String code) {
        Optional<String> cachedUrl = linkCache.get(code);
        if (cachedUrl.isPresent()) {
            incrementClickCount(code);
            return cachedUrl;
        }

        String requestId = MDC.get("requestId");
        Optional<LinkDocument> docOpt = linkRepository.findByShortCode(code);

        if (docOpt.isEmpty()) {
            return Optional.empty();
        }

        LinkDocument doc = docOpt.get();
        if (isExpired(doc)) {
            log.warn("level=WARN code={} event=link_expired requestId={} shortCode={}",
                    LogCodes.WARN_LINK_EXPIRED, requestId, code);
            return Optional.empty();
        }

        Duration cacheTtl = linkCacheTtl;
        if (doc.getExpiresAt() != null) {
            Duration remainingLinkTtl = Duration.between(Instant.now(), doc.getExpiresAt());
            if (remainingLinkTtl.compareTo(cacheTtl) < 0) {
                cacheTtl = remainingLinkTtl;
            }
        }
        linkCache.put(code, doc.getOriginalUrl(), cacheTtl);
        incrementClickCount(code);

        return Optional.of(doc.getOriginalUrl());
    }

    private void incrementClickCount(String code) {
        String requestId = MDC.get("requestId");
        try {
            Query query = new Query(Criteria.where("shortCode").is(code));
            Update update = new Update().inc("clickCount", 1);
            mongoTemplate.updateFirst(query, update, LinkDocument.class);
        } catch (Exception ex) {
            log.error("level=ERROR code={} event=link_persistence_failed requestId={} exception={}",
                    LogCodes.ERR_LINK_PERSISTENCE, requestId, ex.getClass().getSimpleName());
        }
    }

    @Override
    public long purgeExpiredLinks() {
        String requestId = MDC.get("requestId");
        long deletedCount = linkRepository.deleteByExpiresAtBefore(Instant.now());
        log.info("level=INFO event=purge_expired_links requestId={} deletedCount={}", requestId, deletedCount);
        return deletedCount;
    }

    private void validateUrl(URI uri) {
        String requestId = MDC.get("requestId");
        if (uri == null) {
            log.warn("level=WARN code={} event=validation_failed requestId={} message=\"targetUrl is required\"",
                    LogCodes.WARN_INVALID_URL, requestId);
            throw new InvalidUrlException("Target URL cannot be null.");
        }

        String scheme = uri.getScheme();
        if (scheme == null || (!scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https"))) {
            log.warn("level=WARN code={} event=validation_failed requestId={} message=\"Invalid scheme: {}\"",
                    LogCodes.WARN_INVALID_URL, requestId, scheme);
            throw new InvalidUrlException("Target URL scheme must be http or https.");
        }

        if (uri.getHost() == null || uri.getHost().isBlank()) {
            log.warn("level=WARN code={} event=validation_failed requestId={} message=\"Missing host\"",
                    LogCodes.WARN_INVALID_URL, requestId);
            throw new InvalidUrlException("Target URL must have a valid domain host.");
        }
    }


    private LinkResponse toResponse(LinkDocument doc) {
        var resp = new LinkResponse();
        resp.setCode(doc.getShortCode());
        resp.setTargetUrl(URI.create(doc.getOriginalUrl()));
        resp.setShortUrl(URI.create(baseUrl + "/" + doc.getShortCode()));
        resp.setCreatedAt(doc.getCreatedAt().atOffset(ZoneOffset.UTC));

        if (doc.getExpiresAt() != null) {
            resp.setExpiresAt(JsonNullable.of(doc.getExpiresAt().atOffset(ZoneOffset.UTC)));
        } else {
            resp.setExpiresAt(JsonNullable.undefined());
        }

        resp.setClickCount(doc.getClickCount());
        return resp;
    }
    private boolean isExpired(LinkDocument doc) {
        return doc.getExpiresAt() != null && doc.getExpiresAt().isBefore(Instant.now());
    }

}
