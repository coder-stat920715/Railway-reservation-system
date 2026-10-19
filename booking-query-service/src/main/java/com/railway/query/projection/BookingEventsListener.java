package com.railway.query.projection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** CQRS read side: consumes CDC-delivered outbox events from both topics. */
@Component
@RequiredArgsConstructor
@Slf4j
public class BookingEventsListener {

    private final BookingProjector projector;
    private final ObjectMapper mapper;

    @KafkaListener(topics = {"booking-events", "payment-events"}, groupId = "booking-query-service")
    public void onMessage(ConsumerRecord<String, String> record) throws Exception {
        String type = EventHeaders.get(record, "eventType");
        if (type == null || record.value() == null) {
            return;
        }
        UUID eventId = EventHeaders.eventId(record);
        JsonNode payload = mapper.readTree(record.value());
        try {
            projector.project(eventId, type, payload);
        } catch (DataIntegrityViolationException dup) {
            log.info("Event {} already applied concurrently; skipping", eventId);
        }
    }
}
