package com.railway.payment.service;

import com.railway.payment.domain.Payment;
import com.railway.payment.domain.PaymentStatus;
import com.railway.payment.domain.ProcessedEvent;
import com.railway.payment.outbox.OutboxPublisher;
import com.railway.payment.repository.PaymentRepository;
import com.railway.payment.repository.ProcessedEventRepository;
import com.railway.payment.service.PaymentMessages.BookingCancelled;
import com.railway.payment.service.PaymentMessages.PaymentCompleted;
import com.railway.payment.service.PaymentMessages.PaymentFailed;
import com.railway.payment.service.PaymentMessages.PaymentRefunded;
import com.railway.payment.service.PaymentMessages.PaymentRequested;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** SAGA participant. Each handler is idempotent (processed_events PK + unique booking_id on payments). */
@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private static final String AGG = "payment";

    private final PaymentRepository payments;
    private final ProcessedEventRepository processed;
    private final OutboxPublisher outbox;
    private final PaymentGateway gateway;

    @Transactional
    public void processPayment(UUID eventId, PaymentRequested cmd) {
        if (alreadyProcessed(eventId) || payments.findByBookingId(cmd.bookingId()).isPresent()) {
            return;
        }
        PaymentGateway.Result result = gateway.charge(cmd.amount(), cmd.passengerName());
        Payment p = new Payment();
        p.setPaymentId(UUID.randomUUID());
        p.setBookingId(cmd.bookingId());
        p.setAmount(cmd.amount());
        if (result.approved()) {
            p.setStatus(PaymentStatus.COMPLETED);
            payments.save(p);
            outbox.publish(AGG, cmd.bookingId(), "PaymentCompleted",
                    new PaymentCompleted(cmd.bookingId(), p.getPaymentId(), p.getAmount()));
        } else {
            p.setStatus(PaymentStatus.FAILED);
            p.setFailureReason(result.reason());
            payments.save(p);
            outbox.publish(AGG, cmd.bookingId(), "PaymentFailed", new PaymentFailed(cmd.bookingId(), result.reason()));
        }
        log.info("Payment for booking {} -> {}", cmd.bookingId(), p.getStatus());
    }

    /** Compensation: refund a captured payment when its booking is cancelled. */
    @Transactional
    public void refundIfCaptured(UUID eventId, BookingCancelled evt) {
        if (alreadyProcessed(eventId)) {
            return;
        }
        payments.findByBookingId(evt.bookingId())
                .filter(p -> p.getStatus() == PaymentStatus.COMPLETED)
                .ifPresent(p -> {
                    p.setStatus(PaymentStatus.REFUNDED);
                    outbox.publish(AGG, evt.bookingId(), "PaymentRefunded",
                            new PaymentRefunded(evt.bookingId(), p.getPaymentId(), p.getAmount()));
                    log.info("Refunded payment {} for cancelled booking {}", p.getPaymentId(), evt.bookingId());
                });
    }

    private boolean alreadyProcessed(UUID eventId) {
        if (processed.existsById(eventId)) {
            log.debug("Duplicate event {} ignored", eventId);
            return true;
        }
        processed.saveAndFlush(new ProcessedEvent(eventId));
        return false;
    }
}
