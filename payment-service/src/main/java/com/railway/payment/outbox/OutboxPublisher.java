package com.railway.payment.outbox;

import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private final ApplicationEventPublisher publisher;

    /** aggregateType "payment" is routed by Debezium to the payment-events topic. */
    public void publish(String aggregateType, Object aggregateId, String eventType, Object payload) {
        publisher.publishEvent(new OutboxEvent(aggregateType, String.valueOf(aggregateId), eventType, payload));
    }
}
