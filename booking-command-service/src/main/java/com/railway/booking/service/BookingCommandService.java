package com.railway.booking.service;

import com.railway.booking.client.PricingClient;
import com.railway.booking.client.RemoteDtos.Quote;
import com.railway.booking.controller.BookingRequest;
import com.railway.booking.domain.Booking;
import com.railway.booking.repository.BookingRepository;
import com.railway.booking.saga.BookingSagaOrchestrator;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/** CQRS command side. Remote pricing calls happen OUTSIDE the DB transaction; the SAGA start is transactional. */
@Service
@RequiredArgsConstructor
public class BookingCommandService {

    private final PricingClient pricing;
    private final BookingSagaOrchestrator saga;
    private final BookingRepository bookings;

    public Booking createBooking(BookingRequest r) {
        Quote quote = pricing.quote(r.trainId(), r.seatId(), r.fromStation(), r.toStation());
        try {
            return saga.start(r, quote);
        } catch (DataIntegrityViolationException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Seat is already booked for this journey date");
        }
    }

    public Booking get(UUID id) {
        return bookings.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Booking " + id + " not found"));
    }

    public Booking cancel(UUID id, String reason) {
        return saga.cancel(id, reason);
    }
}
