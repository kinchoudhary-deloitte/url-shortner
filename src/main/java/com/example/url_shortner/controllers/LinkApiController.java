package com.example.url_shortner.controllers;

import com.example.demo.api.LinksApi;
import com.example.demo.model.CreateLinkRequest;
import com.example.demo.model.LinkResponse;
import com.example.url_shortner.exceptions.LinkNotFoundException;
import com.example.url_shortner.exceptions.LogCodes;
import com.example.url_shortner.service.LinkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LinkApiController implements LinksApi {

    private static final Logger log = LoggerFactory.getLogger(LinkApiController.class);
    private final LinkService linkService;

    public LinkApiController(LinkService linkService) {
        this.linkService = linkService;
    }

    @Override
    public ResponseEntity<LinkResponse> createShortLink(CreateLinkRequest request) {
        String requestId = MDC.get("requestId");
        log.info("level=INFO event=link_create_requested requestId={}", requestId);

        LinkResponse created = linkService.createLink(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @Override
    public ResponseEntity<LinkResponse> getLinkMetadata(String code) {
        String requestId = MDC.get("requestId");
        log.info("level=INFO event=link_metadata_requested requestId={} shortCode={}", requestId, code);

        return linkService.findByCode(code)
                .map(ResponseEntity::ok)
                .orElseThrow(() -> {
                    log.warn("level=WARN code={} event=link_not_found requestId={} shortCode={}",
                            LogCodes.WARN_LINK_NOT_FOUND, requestId, code);
                    return new LinkNotFoundException("Link not found with code: " + code);
                });
    }
}

