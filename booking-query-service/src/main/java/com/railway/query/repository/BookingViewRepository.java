package com.railway.query.repository;

import com.railway.query.domain.BookingView;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface BookingViewRepository extends JpaRepository<BookingView, UUID> {
    Optional<BookingView> findByPnr(String pnr);

    Page<BookingView> findByPassengerNameContainingIgnoreCase(String passengerName, Pageable pageable);
}
