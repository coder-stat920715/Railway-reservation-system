package com.railway.booking.domain;

/** Booking state machine: CREATED -> PAYMENT_PENDING -> CONFIRMED | CANCELLED; CONFIRMED -> CANCELLED. */
public enum BookingStatus {
    CREATED, PAYMENT_PENDING, CONFIRMED, CANCELLED;

    public boolean canTransitionTo(BookingStatus next) {
        return switch (this) {
            case CREATED -> next == PAYMENT_PENDING || next == CANCELLED;
            case PAYMENT_PENDING -> next == CONFIRMED || next == CANCELLED;
            case CONFIRMED -> next == CANCELLED;
            case CANCELLED -> false;
        };
    }
}
