package com.railway.booking.outbox;

/** In-process application event; converted into an outbox_events row inside the caller's transaction. */
public record OutboxEvent(String aggregateType, String aggregateId, String eventType, Object payload) {
}
