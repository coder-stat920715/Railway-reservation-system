package com.railway.booking.client;

import java.math.BigDecimal;

public final class RemoteDtos {
    private RemoteDtos() {}

    public record JourneyInfo(Long trainId, Long trainTypeId, String fromStation, String toStation,
                              BigDecimal distanceKm) {}

    public record SeatInfo(Long seatId, Integer seatNumber, Long coachId, Long coachTypeId, Long trainId) {}

    public record FareRequest(Long trainTypeId, Long coachTypeId, BigDecimal distanceKm) {}

    public record FareResponse(Long trainTypeId, Long coachTypeId, BigDecimal constantFare, BigDecimal farePerKm,
                               BigDecimal distanceKm, BigDecimal totalFare) {}

    /** Price quote for a seat on a journey segment. */
    public record Quote(BigDecimal distanceKm, Long coachTypeId, BigDecimal totalFare) {}
}
