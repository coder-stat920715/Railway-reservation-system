package com.railway.payment.service;

import java.math.BigDecimal;
import java.util.UUID;

public final class PaymentMessages {
    private PaymentMessages() {}

    // consumed (booking-events)
    public record PaymentRequested(UUID bookingId, String pnr, BigDecimal amount, String passengerName) {}

    public record BookingCancelled(UUID bookingId, String pnr, String reason) {}

    // published (payment-events)
    public record PaymentCompleted(UUID bookingId, UUID paymentId, BigDecimal amount) {}

    public record PaymentFailed(UUID bookingId, String reason) {}

    public record PaymentRefunded(UUID bookingId, UUID paymentId, BigDecimal amount) {}
}
