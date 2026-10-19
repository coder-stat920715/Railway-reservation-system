package com.railway.payment.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
public class OutboxEventListener {

    private final EntityManager em;
    private final ObjectMapper mapper;

    @EventListener
    @Transactional(propagation = Propagation.MANDATORY)
    public void on(OutboxEvent event) {
        String json;
        try {
            json = mapper.writeValueAsString(event.payload());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialise outbox payload for " + event.eventType(), e);
        }
        em.createNativeQuery("""
                INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, created_at, processed)
                VALUES (:id, :aggregateType, :aggregateId, :eventType, CAST(:payload AS jsonb), now(), false)
                """)
                .setParameter("id", UUID.randomUUID())
                .setParameter("aggregateType", event.aggregateType())
                .setParameter("aggregateId", event.aggregateId())
                .setParameter("eventType", event.eventType())
                .setParameter("payload", json)
                .executeUpdate();
    }
}
