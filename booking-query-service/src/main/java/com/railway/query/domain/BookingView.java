package com.railway.query.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "booking_view")
@Getter @Setter @NoArgsConstructor
public class BookingView {

    @Id
    @Column(name = "booking_id")
    private UUID bookingId;
    private String pnr;
    private String status;
    @Column(name = "total_amount")
    private BigDecimal totalAmount;
    @Column(name = "train_id")
    private Long trainId;
    @Column(name = "seat_id")
    private Long seatId;
    @Column(name = "from_station")
    private String fromStation;
    @Column(name = "to_station")
    private String toStation;
    @Column(name = "journey_date")
    private LocalDate journeyDate;
    @Column(name = "passenger_name")
    private String passengerName;
    @Column(name = "payment_status")
    private String paymentStatus;
    @Column(name = "cancellation_reason")
    private String cancellationReason;
    @Column(name = "created_at")
    private Instant createdAt;
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    public BookingView(UUID bookingId) {
        this.bookingId = bookingId;
    }

    @PrePersist
    @PreUpdate
    void touch() {
        updatedAt = Instant.now();
    }
}
