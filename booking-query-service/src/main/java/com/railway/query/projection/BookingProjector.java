package com.railway.query.projection;

import com.fasterxml.jackson.databind.JsonNode;
import com.railway.query.domain.BookingView;
import com.railway.query.domain.ProcessedEvent;
import com.railway.query.repository.BookingViewRepository;
import com.railway.query.repository.ProcessedEventRepository;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Applies domain events to the read model. Idempotent (processed_events PK) and tolerant of cross-topic
 * reordering: a payment event arriving before BookingCreated creates a stub row that is completed later, and
 * booking status only ever moves forward (CREATED < PAYMENT_PENDING < CONFIRMED < CANCELLED).
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class BookingProjector {

    private final BookingViewRepository views;
    private final ProcessedEventRepository processed;

    @Transactional
    public void project(UUID eventId, String type, JsonNode p) {
        if (processed.existsById(eventId)) {
            log.debug("Duplicate event {} ignored", eventId);
            return;
        }
        processed.saveAndFlush(new ProcessedEvent(eventId));

        JsonNode idNode = p.get("bookingId");
        if (idNode == null) {
            return;
        }
        UUID bookingId = UUID.fromString(idNode.asText());
        BookingView v = views.findById(bookingId).orElseGet(() -> new BookingView(bookingId));

        switch (type) {
            case "BookingCreated" -> {
                v.setPnr(text(p, "pnr"));
                v.setTotalAmount(p.hasNonNull("totalAmount") ? p.get("totalAmount").decimalValue() : null);
                v.setTrainId(p.hasNonNull("trainId") ? p.get("trainId").asLong() : null);
                v.setSeatId(p.hasNonNull("seatId") ? p.get("seatId").asLong() : null);
                v.setFromStation(text(p, "fromStation"));
                v.setToStation(text(p, "toStation"));
                v.setJourneyDate(p.hasNonNull("journeyDate") ? LocalDate.parse(text(p, "journeyDate")) : null);
                v.setPassengerName(text(p, "passengerName"));
                v.setCreatedAt(p.hasNonNull("createdAt") ? Instant.parse(text(p, "createdAt")) : null);
                advance(v, text(p, "status"));
            }
            case "BookingConfirmed" -> advance(v, "CONFIRMED");
            case "BookingCancelled" -> {
                advance(v, "CANCELLED");
                v.setCancellationReason(text(p, "reason"));
                if (v.getPnr() == null) {
                    v.setPnr(text(p, "pnr"));
                }
            }
            case "PaymentCompleted" -> v.setPaymentStatus("PAID");
            case "PaymentFailed" -> v.setPaymentStatus("FAILED");
            case "PaymentRefunded" -> v.setPaymentStatus("REFUNDED");
            default -> log.debug("Ignoring event type {}", type);
        }
        views.save(v);
    }

    private static void advance(BookingView v, String next) {
        if (next != null && rank(next) >= rank(v.getStatus())) {
            v.setStatus(next);
        }
    }

    private static int rank(String status) {
        if (status == null) {
            return -1;
        }
        return switch (status) {
            case "CREATED" -> 0;
            case "PAYMENT_PENDING" -> 1;
            case "CONFIRMED" -> 2;
            case "CANCELLED" -> 3;
            default -> -1;
        };
    }

    private static String text(JsonNode n, String field) {
        return n.hasNonNull(field) ? n.get(field).asText() : null;
    }
}
