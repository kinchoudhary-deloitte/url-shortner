package com.example.url_shortner.controllers;

import com.example.url_shortner.kafka.LinkCreateRequested;
import com.example.url_shortner.kafka.NativeKafkaCliProducer;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/v1/events")
public class KafkaProducerController {

    private final NativeKafkaCliProducer producer;

    public KafkaProducerController(NativeKafkaCliProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/link-create-requests")
    public CompletableFuture<ResponseEntity<LinkCreateRequested>> publishLinkCreateRequest(
            @Valid @RequestBody PublishLinkCreateRequest request) {
        return producer.publish(request.targetUrl(), request.customCode(), request.ttlSeconds())
                .thenApply(event -> ResponseEntity.accepted().body(event));
    }

    public record PublishLinkCreateRequest(
            @NotNull URI targetUrl,
            @Pattern(regexp = "^[a-zA-Z0-9_-]{4,16}$") String customCode,
            @NotNull @Positive Long ttlSeconds) {
    }
}
