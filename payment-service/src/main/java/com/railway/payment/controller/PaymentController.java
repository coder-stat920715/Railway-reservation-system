package com.railway.payment.controller;

import com.railway.payment.domain.Payment;
import com.railway.payment.repository.PaymentRepository;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentRepository payments;

    @GetMapping("/booking/{bookingId}")
    public Payment byBooking(@PathVariable("bookingId") UUID bookingId) {
        return payments.findByBookingId(bookingId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "No payment for booking " + bookingId));
    }
}
