package com.railway.booking.outbox;

import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private final ApplicationEventPublisher publisher;

    /**
     * Records a domain event. Must be called inside an active transaction; the row is written atomically with the
     * state change. Debezium later routes it by aggregateType ("booking" -> booking-events).
     */
    public void publish(String aggregateType, Object aggregateId, String eventType, Object payload) {
        publisher.publishEvent(new OutboxEvent(aggregateType, String.valueOf(aggregateId), eventType, payload));
    }
}
