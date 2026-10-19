package com.railway.booking.controller;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.FutureOrPresent;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.LocalDate;

public record BookingRequest(
        @NotNull Long trainId,
        @NotNull Long seatId,
        @NotBlank String fromStation,
        @NotBlank String toStation,
        @NotNull @FutureOrPresent LocalDate journeyDate,
        @NotBlank String passengerName,
        @Email String passengerEmail) {
}
