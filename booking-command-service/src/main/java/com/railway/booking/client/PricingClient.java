package com.railway.booking.client;

import com.railway.booking.client.RemoteDtos.FareRequest;
import com.railway.booking.client.RemoteDtos.FareResponse;
import com.railway.booking.client.RemoteDtos.JourneyInfo;
import com.railway.booking.client.RemoteDtos.Quote;
import com.railway.booking.client.RemoteDtos.SeatInfo;
import feign.FeignException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

/**
 * Resilient facade over the catalog and fare services.
 * Aspect order: Retry( CircuitBreaker( RateLimiter( call ) ) ). The fallback on the outermost aspect translates every
 * failure into a meaningful HTTP status instead of leaking transport exceptions.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PricingClient {

    private final CatalogClient catalog;
    private final FareClient fares;

    @Retry(name = "pricing", fallbackMethod = "quoteFallback")
    @CircuitBreaker(name = "pricing")
    @RateLimiter(name = "pricing")
    public Quote quote(Long trainId, Long seatId, String from, String to) {
        SeatInfo seat = catalog.seat(seatId);
        if (!seat.trainId().equals(trainId)) {
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,
                    "Seat " + seatId + " does not belong to train " + trainId);
        }
        JourneyInfo journey = catalog.journeyInfo(trainId, from, to);
        FareResponse fare = fares.calculate(
                new FareRequest(journey.trainTypeId(), seat.coachTypeId(), journey.distanceKm()));
        return new Quote(journey.distanceKm(), seat.coachTypeId(), fare.totalFare());
    }

    /** Business errors raised locally: rethrow untouched. */
    public Quote quoteFallback(Long trainId, Long seatId, String from, String to, ResponseStatusException e) {
        throw e;
    }

    /** 4xx from a downstream service: surface as a client error. */
    public Quote quoteFallback(Long trainId, Long seatId, String from, String to,
                               FeignException.FeignClientException e) {
        HttpStatus status = HttpStatus.resolve(e.status());
        throw new ResponseStatusException(status != null ? status : HttpStatus.BAD_REQUEST,
                "Pricing request rejected by upstream service (HTTP " + e.status() + ")");
    }

    /** Timeouts, 5xx, open circuit, rate limit exceeded. */
    public Quote quoteFallback(Long trainId, Long seatId, String from, String to, Throwable t) {
        log.warn("Pricing unavailable for train={} seat={}: {}", trainId, seatId, t.toString());
        throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                "Pricing is temporarily unavailable, please retry", t);
    }
}
