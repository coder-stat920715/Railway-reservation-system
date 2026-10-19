package com.railway.booking.repository;

import com.railway.booking.domain.Booking;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface BookingRepository extends JpaRepository<Booking, UUID> {
}
