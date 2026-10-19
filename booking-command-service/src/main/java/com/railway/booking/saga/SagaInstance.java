package com.railway.booking.saga;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "saga_instances")
@Getter @Setter @NoArgsConstructor
public class SagaInstance {

    @Id
    @Column(name = "saga_id")
    private UUID sagaId;

    @Column(name = "booking_id", nullable = false, unique = true)
    private UUID bookingId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private SagaState state;

    @Column(name = "failure_reason")
    private String failureReason;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public SagaInstance(UUID bookingId) {
        this.sagaId = UUID.randomUUID();
        this.bookingId = bookingId;
        this.state = SagaState.STARTED;
    }

    @PrePersist
    void onCreate() {
        createdAt = Instant.now();
        updatedAt = createdAt;
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = Instant.now();
    }
}
