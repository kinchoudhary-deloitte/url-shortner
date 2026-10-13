package com.example.url_shortner.controllers;


import com.example.demo.model.CreateLinkRequest;
import com.example.demo.model.LinkResponse;

import com.example.url_shortner.exceptions.DuplicateCodeException;
import com.example.url_shortner.service.LinkService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.openapitools.jackson.nullable.JsonNullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest({LinkApiController.class, RedirectController.class})
@DisplayName("Link Shortener Controller Tests (JUnit 5)")
class LinkControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private LinkService linkService;

    @Nested
    @DisplayName("POST /api/v1/links")
    class CreateLinkTests {

        @Test
        @DisplayName("Should create short link and return 201")
        void shouldCreateLinkSuccessfully() throws Exception {
            var request = new CreateLinkRequest();
            request.setTargetUrl(URI.create("https://example.com/target"));
            request.setCustomCode("custom1");

            var response = new LinkResponse();
            response.setCode("custom1");
            response.setTargetUrl(URI.create("https://example.com/target"));
            response.setShortUrl(URI.create("http://localhost:8080/custom1"));
            response.setCreatedAt(OffsetDateTime.now());
            response.setExpiresAt(JsonNullable.undefined());
            response.setClickCount(0L);

            when(linkService.createLink(any(CreateLinkRequest.class))).thenReturn(response);

            mockMvc.perform(post("/api/v1/links")
                            .header("X-Request-Id", "test-req-001")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(request)))
                    .andExpect(status().isCreated())
                    .andExpect(header().string("X-Request-Id", "test-req-001"))
                    .andExpect(jsonPath("$.code").value("custom1"))
                    .andExpect(jsonPath("$.shortUrl").value("http://localhost:8080/custom1"));
        }

        @Test
        @DisplayName("Should return 400 when duplicate custom code is provided")
        void shouldReturn400OnDuplicateCode() throws Exception {
            var request = new CreateLinkRequest();
            request.setTargetUrl(URI.create("https://example.com/target"));
            request.setCustomCode("existingCode");

            when(linkService.createLink(any(CreateLinkRequest.class)))
                    .thenThrow(new DuplicateCodeException("Short code 'existingCode' is already in use."));

            mockMvc.perform(post("/api/v1/links")
                            .header("X-Request-Id", "test-req-dup")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(request)))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.code").value("warn_004"));
        }
    }

    @Nested
    @DisplayName("GET /{code}")
    class RedirectTests {

        @Test
        @DisplayName("Should return 302 with Location header when link exists")
        void shouldRedirectWhenFound() throws Exception {
            when(linkService.resolveAndRecordClick("promo24"))
                    .thenReturn(Optional.of("https://example.com/sales"));

            mockMvc.perform(get("/promo24")
                            .header("X-Request-Id", "test-req-002"))
                    .andExpect(status().isFound())
                    .andExpect(header().string("Location", "https://example.com/sales"))
                    .andExpect(header().string("X-Request-Id", "test-req-002"));
        }

        @Test
        @DisplayName("Should return 404 when code does not exist")
        void shouldReturn404WhenNotFound() throws Exception {
            when(linkService.resolveAndRecordClick("missing"))
                    .thenReturn(Optional.empty());

            mockMvc.perform(get("/missing")
                            .header("X-Request-Id", "test-req-003"))
                    .andExpect(status().isNotFound())
                    .andExpect(jsonPath("$.code").value("warn_002"));
        }
    }
}
