package com.railway.booking.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.railway.booking.saga.SagaMessages.PaymentCompleted;
import com.railway.booking.saga.SagaMessages.PaymentFailed;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Drives the SAGA from payment-service results arriving on payment-events (via Debezium). */
@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventListener {

    private final BookingSagaOrchestrator orchestrator;
    private final ObjectMapper mapper;

    @KafkaListener(topics = "payment-events", groupId = "booking-command-service")
    public void onMessage(ConsumerRecord<String, String> record) throws Exception {
        String type = EventHeaders.get(record, "eventType");
        UUID eventId = EventHeaders.eventId(record);
        if (type == null) {
            log.warn("payment-events record without eventType header at offset {}", record.offset());
            return;
        }
        try {
            switch (type) {
                case "PaymentCompleted" -> orchestrator.onPaymentCompleted(eventId,
                        mapper.readValue(record.value(), PaymentCompleted.class));
                case "PaymentFailed" -> orchestrator.onPaymentFailed(eventId,
                        mapper.readValue(record.value(), PaymentFailed.class));
                default -> log.debug("Ignoring payment event type {}", type);
            }
        } catch (DataIntegrityViolationException dup) {
            log.info("Event {} already applied concurrently; skipping", eventId);
        }
    }
}
