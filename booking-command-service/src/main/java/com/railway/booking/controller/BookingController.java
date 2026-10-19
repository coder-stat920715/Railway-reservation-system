package com.railway.booking.controller;

import com.railway.booking.service.BookingCommandService;
import jakarta.validation.Valid;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/bookings")
@RequiredArgsConstructor
public class BookingController {

    private final BookingCommandService service;

    /** Accepted (202): the booking is created and the payment SAGA runs asynchronously. */
    @PostMapping
    public ResponseEntity<BookingResponse> create(@Valid @RequestBody BookingRequest request) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(BookingResponse.from(service.createBooking(request)));
    }

    @GetMapping("/{id}")
    public BookingResponse get(@PathVariable("id") UUID id) {
        return BookingResponse.from(service.get(id));
    }

    @PostMapping("/{id}/cancel")
    public BookingResponse cancel(@PathVariable("id") UUID id,
                                  @RequestParam(value = "reason", defaultValue = "Cancelled by customer") String reason) {
        return BookingResponse.from(service.cancel(id, reason));
    }
}
