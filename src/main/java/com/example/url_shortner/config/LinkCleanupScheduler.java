package com.example.url_shortner.config;



import com.example.url_shortner.service.LinkService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class LinkCleanupScheduler {

    private static final Logger log = LoggerFactory.getLogger(LinkCleanupScheduler.class);
    private final LinkService linkService;

    public LinkCleanupScheduler(LinkService linkService) {
        this.linkService = linkService;
    }

    @Scheduled(cron = "${app.cleanup.cron:0 0 * * * *}")
    public void executePurge() {
        String scheduledRequestId = "sched-" + UUID.randomUUID().toString().substring(0, 8);
        MDC.put("requestId", scheduledRequestId);

        try {
            log.info("level=INFO event=scheduled_cleanup_started requestId={}", scheduledRequestId);
            long purged = linkService.purgeExpiredLinks();
            log.info("level=INFO event=scheduled_cleanup_finished requestId={} purgedCount={}",
                    scheduledRequestId, purged);
        } catch (Exception ex) {
            log.error("level=ERROR code=error_001 event=scheduled_cleanup_failed requestId={} exception={}",
                    scheduledRequestId, ex.getClass().getSimpleName(), ex);
        } finally {
            MDC.remove("requestId");
        }
    }
}
