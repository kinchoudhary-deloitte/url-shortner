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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
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
        @DisplayName("Should trim custom code and apply requested expiry")
        void shouldCreateLinkWithTrimmedCustomCodeAndExpiry() {
            var request = new CreateLinkRequest();
            request.setTargetUrl(URI.create("https://example.com/target"));
            request.setCustomCode("  my-alias  ");
            request.setTtlSeconds(3600L);
            when(linkRepository.existsByShortCode("my-alias")).thenReturn(false);
            when(linkRepository.save(any(LinkDocument.class))).thenAnswer(inv -> inv.getArgument(0));

            Instant beforeCreation = Instant.now();
            LinkResponse response = linkService.createLink(request);
            Instant afterCreation = Instant.now();

            ArgumentCaptor<LinkDocument> documentCaptor = ArgumentCaptor.forClass(LinkDocument.class);
            verify(linkRepository).save(documentCaptor.capture());
            LinkDocument savedDocument = documentCaptor.getValue();
            assertThat(savedDocument.getShortCode()).isEqualTo("my-alias");
            assertThat(savedDocument.getCreatedAt()).isBetween(beforeCreation, afterCreation);
            assertThat(savedDocument.getExpiresAt())
                    .isEqualTo(savedDocument.getCreatedAt().plusSeconds(3600));
            assertThat(response.getShortUrl()).isEqualTo(URI.create("http://localhost:8080/my-alias"));
            verify(linkRepository).existsByShortCode("my-alias");
        }

        @Test
        @DisplayName("Should retry generated code when a candidate already exists")
        void shouldRetryGeneratedCodeAfterCollision() {
            var request = new CreateLinkRequest();
            request.setTargetUrl(URI.create("https://example.com/target"));
            when(linkRepository.existsByShortCode(anyString())).thenReturn(true, false);
            when(linkRepository.save(any(LinkDocument.class))).thenAnswer(inv -> inv.getArgument(0));

            LinkResponse response = linkService.createLink(request);

            assertThat(response.getCode()).hasSize(7).matches("[a-zA-Z0-9]{7}");
            verify(linkRepository, times(2)).existsByShortCode(anyString());
            verify(linkRepository).save(any(LinkDocument.class));
        }

        @Test
        @DisplayName("Should fail when no unique generated code is found after retries")
        void shouldFailWhenGeneratedCodesRemainUnavailable() {
            var request = new CreateLinkRequest();
            request.setTargetUrl(URI.create("https://example.com/target"));
            when(linkRepository.existsByShortCode(anyString())).thenReturn(true);

            assertThatThrownBy(() -> linkService.createLink(request))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Failed to generate a unique short code");

            verify(linkRepository, times(5)).existsByShortCode(anyString());
            verify(linkRepository, never()).save(any(LinkDocument.class));
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
        @DisplayName("Should reject a missing target URL")
        void shouldRejectMissingTargetUrl() {
            var request = new CreateLinkRequest();

            assertThatThrownBy(() -> linkService.createLink(request))
                    .isInstanceOf(InvalidUrlException.class)
                    .hasMessageContaining("cannot be null");
            verifyNoInteractions(linkRepository);
        }

        @Test
        @DisplayName("Should reject a URL without a host")
        void shouldRejectUrlWithoutHost() {
            var request = new CreateLinkRequest();
            request.setTargetUrl(URI.create("http:///target"));

            assertThatThrownBy(() -> linkService.createLink(request))
                    .isInstanceOf(InvalidUrlException.class)
                    .hasMessageContaining("valid domain host");
            verifyNoInteractions(linkRepository);
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
            verify(linkRepository, never()).save(any(LinkDocument.class));
        }

        @Test
        @DisplayName("Should translate a duplicate-key save race to DuplicateCodeException")
        void shouldTranslateDuplicateKeyFromSave() {
            var request = new CreateLinkRequest();
            request.setTargetUrl(URI.create("https://example.com/target"));
            request.setCustomCode("my-alias");
            when(linkRepository.existsByShortCode("my-alias")).thenReturn(false);
            when(linkRepository.save(any(LinkDocument.class)))
                    .thenThrow(new DuplicateKeyException("duplicate short code"));

            assertThatThrownBy(() -> linkService.createLink(request))
                    .isInstanceOf(DuplicateCodeException.class)
                    .hasMessageContaining("my-alias")
                    .hasMessageContaining("already in use");
        }
    }

    @Nested
    @DisplayName("findByCode()")
    class FindByCodeTests {

        @Test
        @DisplayName("Should return link details for an active code")
        void shouldReturnActiveLink() {
            Instant createdAt = Instant.parse("2025-01-01T12:00:00Z");
            var doc = new LinkDocument("active", "https://example.com/dest", createdAt, null);
            doc.setClickCount(3);
            when(linkRepository.findByShortCode("active")).thenReturn(Optional.of(doc));

            Optional<LinkResponse> result = linkService.findByCode("active");

            assertThat(result).isPresent();
            assertThat(result.get().getCode()).isEqualTo("active");
            assertThat(result.get().getTargetUrl()).isEqualTo(URI.create("https://example.com/dest"));
            assertThat(result.get().getShortUrl()).isEqualTo(URI.create("http://localhost:8080/active"));
            assertThat(result.get().getClickCount()).isEqualTo(3);
        }

        @Test
        @DisplayName("Should return empty when code does not exist")
        void shouldReturnEmptyForUnknownCode() {
            when(linkRepository.findByShortCode("missing")).thenReturn(Optional.empty());

            assertThat(linkService.findByCode("missing")).isEmpty();
        }

        @Test
        @DisplayName("Should return empty when requested link has expired")
        void shouldReturnEmptyForExpiredLink() {
            Instant expiredAt = Instant.now().minusSeconds(60);
            var doc = new LinkDocument("expired", "https://example.com/dest",
                    expiredAt.minusSeconds(60), expiredAt);
            when(linkRepository.findByShortCode("expired")).thenReturn(Optional.of(doc));

            assertThat(linkService.findByCode("expired")).isEmpty();
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

        @Test
        @DisplayName("Should return empty when code does not exist")
        void shouldReturnEmptyForUnknownCode() {
            when(linkRepository.findByShortCode("missing")).thenReturn(Optional.empty());

            assertThat(linkService.resolveAndRecordClick("missing")).isEmpty();
            verifyNoInteractions(mongoTemplate);
        }

        @Test
        @DisplayName("Should return destination even if click counter update fails")
        void shouldReturnDestinationWhenClickUpdateFails() {
            var doc = new LinkDocument("myCode", "https://example.com/dest", Instant.now(), null);
            when(linkRepository.findByShortCode("myCode")).thenReturn(Optional.of(doc));
            when(mongoTemplate.updateFirst(any(Query.class), any(Update.class), eq(LinkDocument.class)))
                    .thenThrow(new RuntimeException("database unavailable"));

            Optional<String> result = linkService.resolveAndRecordClick("myCode");

            assertThat(result).contains("https://example.com/dest");
            verify(mongoTemplate).updateFirst(any(Query.class), any(Update.class), eq(LinkDocument.class));
        }
    }

    @Nested
    @DisplayName("purgeExpiredLinks()")
    class PurgeExpiredLinksTests {

        @Test
        @DisplayName("Should return the number of deleted expired links")
        void shouldReturnDeletedLinkCount() {
            when(linkRepository.deleteByExpiresAtBefore(any(Instant.class))).thenReturn(4L);

            assertThat(linkService.purgeExpiredLinks()).isEqualTo(4L);
            verify(linkRepository).deleteByExpiresAtBefore(any(Instant.class));
        }
    }
}
