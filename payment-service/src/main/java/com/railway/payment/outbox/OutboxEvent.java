package com.railway.payment.outbox;

public record OutboxEvent(String aggregateType, String aggregateId, String eventType, Object payload) {
}
