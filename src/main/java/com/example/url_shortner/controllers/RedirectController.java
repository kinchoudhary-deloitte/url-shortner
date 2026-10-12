package com.example.url_shortner.controllers;

import com.example.demo.api.RedirectApi;
import com.example.url_shortner.exceptions.LogCodes;
import com.example.url_shortner.exceptions.LinkNotFoundException;
import com.example.url_shortner.service.LinkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

import org.springframework.http.HttpHeaders;

import java.net.URI;

@RestController
public class RedirectController implements RedirectApi {

    private static final Logger log = LoggerFactory.getLogger(RedirectController.class);
    private final LinkService linkService;

    public RedirectController(LinkService linkService) {
        this.linkService = linkService;
    }

    @Override
    public ResponseEntity<Void> redirectToTarget(String code) {
        String requestId = MDC.get("requestId");

        return linkService.resolveAndRecordClick(code)
                .map(targetUrl -> {
                    log.info("event=link_redirected requestId={} shortCode={}", requestId, code);
                    HttpHeaders headers = new HttpHeaders();
                    headers.setLocation(URI.create(targetUrl));
                    return new ResponseEntity<Void>(headers, HttpStatus.FOUND);
                })
                .orElseThrow(() -> {
                    log.warn("code={} event=link_not_found requestId={} shortCode={}",
                            LogCodes.WARN_LINK_NOT_FOUND, requestId, code);
                    return new LinkNotFoundException("Link code not found: " + code);
                });
    }
}
