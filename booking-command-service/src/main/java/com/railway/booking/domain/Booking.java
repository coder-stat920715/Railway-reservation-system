package com.railway.booking.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "bookings")
@Getter @Setter @NoArgsConstructor
public class Booking {

    @Id
    @Column(name = "booking_id")
    private UUID bookingId;

    @Column(nullable = false, unique = true, length = 10)
    private String pnr;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private BookingStatus status;

    @Column(name = "total_amount", nullable = false, precision = 12, scale = 2)
    private BigDecimal totalAmount;

    @Column(name = "train_id", nullable = false)
    private Long trainId;

    @Column(name = "seat_id", nullable = false)
    private Long seatId;

    @Column(name = "coach_type_id", nullable = false)
    private Long coachTypeId;

    @Column(name = "from_station", nullable = false)
    private String fromStation;

    @Column(name = "to_station", nullable = false)
    private String toStation;

    @Column(name = "journey_date", nullable = false)
    private LocalDate journeyDate;

    @Column(name = "passenger_name", nullable = false)
    private String passengerName;

    @Column(name = "passenger_email")
    private String passengerEmail;

    @Column(name = "distance_km", nullable = false)
    private BigDecimal distanceKm;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Version
    private Long version;

    @PrePersist
    void onCreate() {
        createdAt = Instant.now();
        updatedAt = createdAt;
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = Instant.now();
    }

    public void transitionTo(BookingStatus next) {
        if (!status.canTransitionTo(next)) {
            throw new IllegalStateException("Illegal booking transition " + status + " -> " + next);
        }
        this.status = next;
    }
}
