package com.example.url_shortner.service;

import com.example.demo.model.CreateLinkRequest;
import com.example.demo.model.LinkResponse;

import java.util.Optional;

public interface LinkService {
    LinkResponse createLink(CreateLinkRequest request);
    Optional<LinkResponse> findByCode(String code);
    Optional<String> resolveAndRecordClick(String code);
    long purgeExpiredLinks();
}