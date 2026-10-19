package com.railway.query.controller;

import com.railway.query.domain.BookingView;
import com.railway.query.repository.BookingViewRepository;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PagedModel;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/query/bookings")
@RequiredArgsConstructor
public class BookingQueryController {

    private final BookingViewRepository views;

    @GetMapping("/{id}")
    public BookingView byId(@PathVariable("id") UUID id) {
        return views.findById(id).orElseThrow(() -> notFound("Booking " + id));
    }

    @GetMapping("/pnr/{pnr}")
    public BookingView byPnr(@PathVariable("pnr") String pnr) {
        return views.findByPnr(pnr).orElseThrow(() -> notFound("PNR " + pnr));
    }

    @GetMapping
    public PagedModel<BookingView> search(@RequestParam(value = "passengerName", defaultValue = "") String passengerName,
                                          Pageable pageable) {
        return new PagedModel<>(views.findByPassengerNameContainingIgnoreCase(passengerName, pageable));
    }

    private static ResponseStatusException notFound(String what) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, what + " not found (the read model is eventually consistent)");
    }
}
