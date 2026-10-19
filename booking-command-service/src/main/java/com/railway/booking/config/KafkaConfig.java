package com.railway.booking.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    @Bean
    NewTopic bookingEventsTopic() {
        return TopicBuilder.name("booking-events").partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic paymentEventsTopic() {
        return TopicBuilder.name("payment-events").partitions(3).replicas(1).build();
    }

    /** Retry a failing record 5 times (1s apart) before skipping it; the idempotency table makes retries safe. */
    @Bean
    DefaultErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(1000L, 5L));
    }
}
