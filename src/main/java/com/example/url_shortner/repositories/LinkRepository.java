package com.example.url_shortner.repositories;

import com.example.url_shortner.models.LinkDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.time.Instant;

@Repository
public interface LinkRepository extends MongoRepository<LinkDocument, String> {
    Optional<LinkDocument> findByShortCode(String shortCode);

    boolean existsByShortCode(String shortCode);
    long deleteByExpiresAtBefore(Instant now);
}