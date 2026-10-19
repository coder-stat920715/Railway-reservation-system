package com.railway.booking.saga;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

/** Event/command payloads exchanged through the outbox and Kafka. */
public final class SagaMessages {
    private SagaMessages() {}

    // published by booking-command-service (topic: booking-events)
    public record BookingCreated(UUID bookingId, String pnr, String status, BigDecimal totalAmount, Long trainId,
                                 Long seatId, String fromStation, String toStation, LocalDate journeyDate,
                                 String passengerName, Instant createdAt) {}

    public record PaymentRequested(UUID bookingId, String pnr, BigDecimal amount, String passengerName) {}

    public record BookingConfirmed(UUID bookingId, String pnr) {}

    public record BookingCancelled(UUID bookingId, String pnr, String reason) {}

    // consumed from payment-service (topic: payment-events)
    public record PaymentCompleted(UUID bookingId, UUID paymentId, BigDecimal amount) {}

    public record PaymentFailed(UUID bookingId, String reason) {}
}
