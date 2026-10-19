package com.railway.fare.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "fare_rules")
@Getter @Setter @NoArgsConstructor
public class FareRule {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(name = "train_type_id", nullable = false)
    private Long trainTypeId;
    @Column(name = "coach_type_id", nullable = false)
    private Long coachTypeId;
    @Column(name = "constant_fare", nullable = false)
    private BigDecimal constantFare;
    @Column(name = "fare_per_km", nullable = false)
    private BigDecimal farePerKm;
}
