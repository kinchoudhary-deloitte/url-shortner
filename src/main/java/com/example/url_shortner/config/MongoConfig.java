package com.example.url_shortner.config;



import com.example.url_shortner.models.LinkDocument;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;

import java.util.concurrent.TimeUnit;

@Configuration
public class MongoConfig {

    private final MongoTemplate mongoTemplate;

    public MongoConfig(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @PostConstruct
    public void initIndexes() {
        // 1. Unique index on shortCode
        mongoTemplate.indexOps(LinkDocument.class).ensureIndex(
                new Index().on("shortCode", Sort.Direction.ASC).unique()
        );

        // 2. TTL index on expiresAt (MongoDB removes expired documents automatically)
        mongoTemplate.indexOps(LinkDocument.class).ensureIndex(
                new Index().on("expiresAt", Sort.Direction.ASC).expire(0, TimeUnit.SECONDS)
        );
    }
}

