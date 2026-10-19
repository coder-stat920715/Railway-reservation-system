package com.railway.catalog.service;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.math.BigDecimal;
import java.time.LocalTime;

public final class Dtos {
    private Dtos() {}

    public record TrainRequest(@NotBlank String trainNumber, @NotBlank String name, @NotNull Long trainTypeId) {}

    public record CoachRequest(@NotNull Long coachTypeId, @NotBlank String coachNumber) {}

    public record RouteRequest(@NotNull @Positive Integer stopSequence, @NotBlank String stationCode,
                               @NotBlank String stationName, @NotNull @PositiveOrZero BigDecimal distanceFromOriginKm,
                               LocalTime arrivalTime, LocalTime departureTime) {}

    /** Distance information for a journey segment; consumed by booking-command-service. */
    public record JourneyInfo(Long trainId, Long trainTypeId, String fromStation, String toStation,
                              BigDecimal distanceKm) {}

    /** Seat lookup result; consumed by booking-command-service to validate seat/train and find the coach type. */
    public record SeatInfo(Long seatId, Integer seatNumber, Long coachId, Long coachTypeId, Long trainId) {}
}
