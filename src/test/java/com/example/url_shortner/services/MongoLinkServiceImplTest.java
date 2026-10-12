package com.example.url_shortner.services;

import com.example.demo.model.CreateLinkRequest;
import com.example.demo.model.LinkResponse;
import com.example.url_shortner.exceptions.DuplicateCodeException;
import com.example.url_shortner.exceptions.InvalidUrlException;
import com.example.url_shortner.models.LinkDocument;
import com.example.url_shortner.repositories.LinkRepository;
import com.example.url_shortner.service.MongoLinkServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.net.URI;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("MongoLinkServiceImpl Unit Tests (JUnit 5)")
class MongoLinkServiceImplTest {

    @Mock
    private LinkRepository linkRepository;

    @Mock
    private MongoTemplate mongoTemplate;

    private MongoLinkServiceImpl linkService;

    @BeforeEach
    void setUp() {
        linkService = new MongoLinkServiceImpl(linkRepository, mongoTemplate, "http://localhost:8080");
    }

    @Nested
    @DisplayName("createLink()")
    class CreateLinkTests {

        @Test
        @DisplayName("Should successfully create a link with generated code")
        void shouldCreateLinkSuccessfully() {
            var request = new CreateLinkRequest();
            request.setTargetUrl(URI.create("https://example.com/target"));

            when(linkRepository.existsByShortCode(any())).thenReturn(false);
            when(linkRepository.save(any(LinkDocument.class))).thenAnswer(inv -> inv.getArgument(0));

            LinkResponse response = linkService.createLink(request);

            assertThat(response).isNotNull();
            assertThat(response.getCode()).isNotBlank();
            assertThat(response.getTargetUrl()).isEqualTo(URI.create("https://example.com/target"));
            verify(linkRepository, times(1)).save(any(LinkDocument.class));
        }

        @Test
        @DisplayName("Should reject invalid URL scheme")
        void shouldRejectInvalidScheme() {
            var request = new CreateLinkRequest();
            request.setTargetUrl(URI.create("ftp://example.com/target"));

            assertThatThrownBy(() -> linkService.createLink(request))
                    .isInstanceOf(InvalidUrlException.class)
                    .hasMessageContaining("scheme must be http or https");
        }

        @Test
        @DisplayName("Should throw DuplicateCodeException when custom code already exists")
        void shouldRejectDuplicateCustomCode() {
            var request = new CreateLinkRequest();
            request.setTargetUrl(URI.create("https://example.com/target"));
            request.setCustomCode("my-alias");

            when(linkRepository.existsByShortCode("my-alias")).thenReturn(true);

            assertThatThrownBy(() -> linkService.createLink(request))
                    .isInstanceOf(DuplicateCodeException.class)
                    .hasMessageContaining("already in use");
        }
    }

    @Nested
    @DisplayName("resolveAndRecordClick()")
    class ResolveClickTests {

        @Test
        @DisplayName("Should increment click count and return destination URL")
        void shouldResolveAndIncrementClick() {
            var doc = new LinkDocument("myCode", "https://example.com/dest", Instant.now(), null);
            when(linkRepository.findByShortCode("myCode")).thenReturn(Optional.of(doc));

            Optional<String> result = linkService.resolveAndRecordClick("myCode");

            assertThat(result).isPresent().contains("https://example.com/dest");
            verify(mongoTemplate, times(1)).updateFirst(any(Query.class), any(Update.class), eq(LinkDocument.class));
        }

        @Test
        @DisplayName("Should return empty when link has expired")
        void shouldReturnEmptyForExpiredLink() {
            Instant expiredTime = Instant.now().minusSeconds(3600);
            var doc = new LinkDocument("oldCode", "https://example.com/dest", Instant.now().minusSeconds(7200), expiredTime);
            when(linkRepository.findByShortCode("oldCode")).thenReturn(Optional.of(doc));

            Optional<String> result = linkService.resolveAndRecordClick("oldCode");

            assertThat(result).isEmpty();
            verify(mongoTemplate, never()).updateFirst(any(), any(), eq(LinkDocument.class));
        }
    }
}

