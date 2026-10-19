package com.railway.payment.messaging;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

public final class EventHeaders {
    private EventHeaders() {}

    public static String get(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        if (h == null || h.value() == null) {
            return null;
        }
        String v = new String(h.value(), StandardCharsets.UTF_8);
        return v.length() > 1 && v.startsWith("\"") && v.endsWith("\"") ? v.substring(1, v.length() - 1) : v;
    }

    public static UUID eventId(ConsumerRecord<?, ?> record) {
        String v = get(record, "eventId");
        if (v != null) {
            try {
                return UUID.fromString(v);
            } catch (IllegalArgumentException ignored) {
                // fall through
            }
        }
        return UUID.nameUUIDFromBytes((record.topic() + "-" + record.partition() + "-" + record.offset())
                .getBytes(StandardCharsets.UTF_8));
    }
}
