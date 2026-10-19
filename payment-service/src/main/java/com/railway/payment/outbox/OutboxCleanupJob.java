package com.railway.payment.outbox;

import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxCleanupJob {

    private final EntityManager em;

    @Scheduled(fixedDelayString = "PT10M", initialDelayString = "PT2M")
    @Transactional
    public void purge() {
        int n = em.createNativeQuery("DELETE FROM outbox_events WHERE created_at < now() - interval '1 hour'")
                .executeUpdate();
        if (n > 0) {
            log.info("Purged {} old outbox rows", n);
        }
    }
}
