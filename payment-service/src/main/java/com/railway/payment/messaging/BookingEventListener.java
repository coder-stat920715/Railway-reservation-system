package com.railway.payment.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.railway.payment.service.PaymentMessages.BookingCancelled;
import com.railway.payment.service.PaymentMessages.PaymentRequested;
import com.railway.payment.service.PaymentService;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class BookingEventListener {

    private final PaymentService paymentService;
    private final ObjectMapper mapper;

    @KafkaListener(topics = "booking-events", groupId = "payment-service")
    public void onMessage(ConsumerRecord<String, String> record) throws Exception {
        String type = EventHeaders.get(record, "eventType");
        UUID eventId = EventHeaders.eventId(record);
        if (type == null) {
            return;
        }
        try {
            switch (type) {
                case "PaymentRequested" -> paymentService.processPayment(eventId,
                        mapper.readValue(record.value(), PaymentRequested.class));
                case "BookingCancelled" -> paymentService.refundIfCaptured(eventId,
                        mapper.readValue(record.value(), BookingCancelled.class));
                default -> log.debug("Ignoring booking event type {}", type);
            }
        } catch (DataIntegrityViolationException dup) {
            log.info("Event {} already applied concurrently; skipping", eventId);
        }
    }
}
