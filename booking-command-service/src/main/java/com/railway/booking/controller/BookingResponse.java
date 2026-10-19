package com.railway.booking.controller;

import com.railway.booking.domain.Booking;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record BookingResponse(UUID bookingId, String pnr, String status, BigDecimal totalAmount, Instant createdAt) {

    public static BookingResponse from(Booking b) {
        return new BookingResponse(b.getBookingId(), b.getPnr(), b.getStatus().name(), b.getTotalAmount(),
                b.getCreatedAt());
    }
}
