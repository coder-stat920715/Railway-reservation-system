package com.railway.catalog.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.LocalTime;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** JPA entities of the catalog bounded context. Cross-aggregate references use plain foreign-key ids. */
public final class Entities {
    private Entities() {}

    @Entity @Table(name = "train_types") @Getter @Setter @NoArgsConstructor
    public static class TrainType {
        @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
        private Long id;
        @Column(nullable = false, unique = true)
        private String name;
        private String description;
    }

    @Entity @Table(name = "coach_types") @Getter @Setter @NoArgsConstructor
    public static class CoachType {
        @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
        private Long id;
        @Column(nullable = false, unique = true)
        private String name;
        @Column(name = "seat_capacity", nullable = false)
        private Integer seatCapacity;
    }

    @Entity @Table(name = "trains") @Getter @Setter @NoArgsConstructor
    public static class Train {
        @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
        private Long id;
        @Column(name = "train_number", nullable = false, unique = true)
        private String trainNumber;
        @Column(nullable = false)
        private String name;
        @Column(name = "train_type_id", nullable = false)
        private Long trainTypeId;
    }

    @Entity @Table(name = "train_coaches") @Getter @Setter @NoArgsConstructor
    public static class TrainCoach {
        @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
        private Long id;
        @Column(name = "train_id", nullable = false)
        private Long trainId;
        @Column(name = "coach_type_id", nullable = false)
        private Long coachTypeId;
        @Column(name = "coach_number", nullable = false)
        private String coachNumber;
    }

    @Entity @Table(name = "seats") @Getter @Setter @NoArgsConstructor
    public static class Seat {
        @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
        private Long id;
        @Column(name = "coach_id", nullable = false)
        private Long coachId;
        @Column(name = "seat_number", nullable = false)
        private Integer seatNumber;
    }

    @Entity @Table(name = "routes") @Getter @Setter @NoArgsConstructor
    public static class Route {
        @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
        private Long id;
        @Column(name = "train_id", nullable = false)
        private Long trainId;
        @Column(name = "stop_sequence", nullable = false)
        private Integer stopSequence;
        @Column(name = "station_code", nullable = false)
        private String stationCode;
        @Column(name = "station_name", nullable = false)
        private String stationName;
        @Column(name = "distance_from_origin_km", nullable = false)
        private BigDecimal distanceFromOriginKm;
        @Column(name = "arrival_time")
        private LocalTime arrivalTime;
        @Column(name = "departure_time")
        private LocalTime departureTime;
    }
}
