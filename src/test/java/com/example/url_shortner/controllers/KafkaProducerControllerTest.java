package com.example.url_shortner.controllers;

import com.example.url_shortner.kafka.LinkCreateRequested;
import com.example.url_shortner.kafka.NativeKafkaCliProducer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;

@WebMvcTest(KafkaProducerController.class)
class KafkaProducerControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private NativeKafkaCliProducer producer;

    @Test
    void publishesEventAndReturnsAcceptedAfterProducerCompletes() throws Exception {
        URI targetUrl = URI.create("https://example.com/target");
        var event = LinkCreateRequested.of(targetUrl, "custom1", 60L);
        when(producer.publish(targetUrl, "custom1", 60L))
                .thenReturn(CompletableFuture.completedFuture(event));

        MvcResult result = mockMvc.perform(post("/api/v1/events/link-create-requests")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"targetUrl":"https://example.com/target","customCode":"custom1","ttlSeconds":60}
                                """))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(result))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").value(event.eventId().toString()))
                .andExpect(jsonPath("$.customCode").value("custom1"))
                .andExpect(jsonPath("$.targetUrl").value(targetUrl.toString()))
                .andExpect(jsonPath("$.ttlSeconds").value(60));

        verify(producer).publish(targetUrl, "custom1", 60L);
    }

    @Test
    void rejectsInvalidRequestWithoutPublishing() throws Exception {
        mockMvc.perform(post("/api/v1/events/link-create-requests")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"targetUrl":"https://example.com/target","customCode":"x","ttlSeconds":0}
                                """))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(producer);
    }
}
