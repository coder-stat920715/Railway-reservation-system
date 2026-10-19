package com.railway.booking.saga;

import com.railway.booking.client.RemoteDtos.Quote;
import com.railway.booking.controller.BookingRequest;
import com.railway.booking.domain.Booking;
import com.railway.booking.domain.BookingStatus;
import com.railway.booking.domain.ProcessedEvent;
import com.railway.booking.outbox.OutboxPublisher;
import com.railway.booking.repository.BookingRepository;
import com.railway.booking.repository.ProcessedEventRepository;
import com.railway.booking.repository.SagaInstanceRepository;
import com.railway.booking.saga.SagaMessages.BookingCancelled;
import com.railway.booking.saga.SagaMessages.BookingConfirmed;
import com.railway.booking.saga.SagaMessages.BookingCreated;
import com.railway.booking.saga.SagaMessages.PaymentCompleted;
import com.railway.booking.saga.SagaMessages.PaymentFailed;
import com.railway.booking.saga.SagaMessages.PaymentRequested;
import java.security.SecureRandom;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

/**
 * SAGA orchestrator.
 * Happy path:  BOOKING_CREATED -> PAYMENT_PENDING (PaymentRequested) -> BOOKING_CONFIRMED
 * Compensation: PaymentFailed -> booking CANCELLED (seat released via partial unique index) -> BookingCancelled
 * Every state change and its outbox event(s) commit in a single ACID transaction.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class BookingSagaOrchestrator {

    private static final String AGG = "booking";
    private static final SecureRandom RANDOM = new SecureRandom();

    private final BookingRepository bookings;
    private final SagaInstanceRepository sagas;
    private final ProcessedEventRepository processed;
    private final OutboxPublisher outbox;

    @Transactional
    public Booking start(BookingRequest r, Quote quote) {
        Booking b = new Booking();
        b.setBookingId(UUID.randomUUID());
        b.setPnr(String.format("%010d", RANDOM.nextLong(0, 10_000_000_000L)));
        b.setStatus(BookingStatus.CREATED);
        b.setTotalAmount(quote.totalFare());
        b.setTrainId(r.trainId());
        b.setSeatId(r.seatId());
        b.setCoachTypeId(quote.coachTypeId());
        b.setFromStation(r.fromStation());
        b.setToStation(r.toStation());
        b.setJourneyDate(r.journeyDate());
        b.setPassengerName(r.passengerName());
        b.setPassengerEmail(r.passengerEmail());
        b.setDistanceKm(quote.distanceKm());
        b = bookings.saveAndFlush(b); // fails fast on the unique active-seat index

        SagaInstance saga = new SagaInstance(b.getBookingId());
        outbox.publish(AGG, b.getBookingId(), "BookingCreated", new BookingCreated(b.getBookingId(), b.getPnr(),
                b.getStatus().name(), b.getTotalAmount(), b.getTrainId(), b.getSeatId(), b.getFromStation(),
                b.getToStation(), b.getJourneyDate(), b.getPassengerName(), b.getCreatedAt()));

        b.transitionTo(BookingStatus.PAYMENT_PENDING);
        saga.setState(SagaState.PAYMENT_PENDING);
        sagas.save(saga);
        outbox.publish(AGG, b.getBookingId(), "PaymentRequested",
                new PaymentRequested(b.getBookingId(), b.getPnr(), b.getTotalAmount(), b.getPassengerName()));
        return b;
    }

    @Transactional
    public void onPaymentCompleted(UUID eventId, PaymentCompleted e) {
        if (alreadyProcessed(eventId)) {
            return;
        }
        Booking b = bookings.findById(e.bookingId()).orElse(null);
        SagaInstance saga = sagas.findByBookingId(e.bookingId()).orElse(null);
        if (b == null || saga == null) {
            log.warn("PaymentCompleted for unknown booking {}", e.bookingId());
            return;
        }
        if (b.getStatus() == BookingStatus.CANCELLED) {
            // Customer cancelled while payment was in flight: re-emit cancellation so payment-service refunds.
            outbox.publish(AGG, b.getBookingId(), "BookingCancelled",
                    new BookingCancelled(b.getBookingId(), b.getPnr(), "Payment received after cancellation"));
            return;
        }
        b.transitionTo(BookingStatus.CONFIRMED);
        saga.setState(SagaState.COMPLETED);
        outbox.publish(AGG, b.getBookingId(), "BookingConfirmed", new BookingConfirmed(b.getBookingId(), b.getPnr()));
        log.info("Booking {} confirmed", b.getBookingId());
    }

    /** Compensating transaction: cancel the booking and release the seat. */
    @Transactional
    public void onPaymentFailed(UUID eventId, PaymentFailed e) {
        if (alreadyProcessed(eventId)) {
            return;
        }
        Booking b = bookings.findById(e.bookingId()).orElse(null);
        SagaInstance saga = sagas.findByBookingId(e.bookingId()).orElse(null);
        if (b == null || saga == null) {
            log.warn("PaymentFailed for unknown booking {}", e.bookingId());
            return;
        }
        if (b.getStatus().canTransitionTo(BookingStatus.CANCELLED)) {
            b.transitionTo(BookingStatus.CANCELLED);
            saga.setState(SagaState.COMPENSATED);
            saga.setFailureReason(e.reason());
            outbox.publish(AGG, b.getBookingId(), "BookingCancelled",
                    new BookingCancelled(b.getBookingId(), b.getPnr(), "Payment failed: " + e.reason()));
            log.info("Booking {} cancelled (payment failed: {})", b.getBookingId(), e.reason());
        }
    }

    /** Customer-initiated cancellation; payment-service refunds if a payment was captured. */
    @Transactional
    public Booking cancel(UUID bookingId, String reason) {
        Booking b = bookings.findById(bookingId).orElseThrow(
                () -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Booking " + bookingId + " not found"));
        if (!b.getStatus().canTransitionTo(BookingStatus.CANCELLED)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Booking is already " + b.getStatus());
        }
        b.transitionTo(BookingStatus.CANCELLED);
        sagas.findByBookingId(bookingId).ifPresent(s -> {
            s.setState(SagaState.COMPENSATED);
            s.setFailureReason(reason);
        });
        outbox.publish(AGG, b.getBookingId(), "BookingCancelled", new BookingCancelled(b.getBookingId(), b.getPnr(), reason));
        return b;
    }

    /** Consumer-side idempotency: PK on processed_events guarantees at-most-once application. */
    private boolean alreadyProcessed(UUID eventId) {
        if (processed.existsById(eventId)) {
            log.debug("Duplicate event {} ignored", eventId);
            return true;
        }
        processed.saveAndFlush(new ProcessedEvent(eventId));
        return false;
    }
}
