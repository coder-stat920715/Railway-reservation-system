package com.railway.payment.service;

import java.math.BigDecimal;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Simulated PSP. Declines amounts above the limit or passengers whose name starts with the configured prefix. */
@Component
public class PaymentGateway {

    public record Result(boolean approved, String reason) {}

    @Value("${payment.max-amount:100000}")
    private BigDecimal maxAmount;

    @Value("${payment.decline-name-prefix:FAIL}")
    private String declinePrefix;

    public Result charge(BigDecimal amount, String passengerName) {
        if (amount.compareTo(maxAmount) > 0) {
            return new Result(false, "Amount exceeds limit");
        }
        if (passengerName != null && passengerName.toUpperCase().startsWith(declinePrefix.toUpperCase())) {
            return new Result(false, "Card declined");
        }
        return new Result(true, null);
    }
}
