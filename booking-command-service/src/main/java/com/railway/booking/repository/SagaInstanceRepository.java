package com.railway.booking.repository;

import com.railway.booking.saga.SagaInstance;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SagaInstanceRepository extends JpaRepository<SagaInstance, UUID> {
    Optional<SagaInstance> findByBookingId(UUID bookingId);
}
