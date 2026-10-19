package com.railway.fare.service;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.math.BigDecimal;

public final class FareDtos {
    private FareDtos() {}

    public record FareRequest(@NotNull Long trainTypeId, @NotNull Long coachTypeId,
                              @NotNull @Positive BigDecimal distanceKm) {}

    public record FareResponse(Long trainTypeId, Long coachTypeId, BigDecimal constantFare, BigDecimal farePerKm,
                               BigDecimal distanceKm, BigDecimal totalFare) {}

    public record FareRuleRequest(@NotNull Long trainTypeId, @NotNull Long coachTypeId,
                                  @NotNull @PositiveOrZero BigDecimal constantFare,
                                  @NotNull @PositiveOrZero BigDecimal farePerKm) {}
}
