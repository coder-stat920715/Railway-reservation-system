package com.railway.fare.service;

import com.railway.fare.domain.FareRule;
import com.railway.fare.repository.FareRuleRepository;
import com.railway.fare.service.FareDtos.FareRequest;
import com.railway.fare.service.FareDtos.FareResponse;
import com.railway.fare.service.FareDtos.FareRuleRequest;
import java.math.BigDecimal;
import java.math.RoundingMode;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
@RequiredArgsConstructor
public class FareService {

    private final FareRuleRepository rules;

    /** total_fare = constant_fare + distance_km * fare_per_km */
    @Transactional(readOnly = true)
    public FareResponse calculate(FareRequest r) {
        FareRule rule = rules.findByTrainTypeIdAndCoachTypeId(r.trainTypeId(), r.coachTypeId())
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "No fare rule for trainType=" + r.trainTypeId() + ", coachType=" + r.coachTypeId()));
        BigDecimal total = rule.getConstantFare()
                .add(r.distanceKm().multiply(rule.getFarePerKm()))
                .setScale(2, RoundingMode.HALF_UP);
        return new FareResponse(r.trainTypeId(), r.coachTypeId(), rule.getConstantFare(), rule.getFarePerKm(),
                r.distanceKm(), total);
    }

    @Transactional
    public FareRule upsert(FareRuleRequest r) {
        FareRule rule = rules.findByTrainTypeIdAndCoachTypeId(r.trainTypeId(), r.coachTypeId())
                .orElseGet(FareRule::new);
        rule.setTrainTypeId(r.trainTypeId());
        rule.setCoachTypeId(r.coachTypeId());
        rule.setConstantFare(r.constantFare());
        rule.setFarePerKm(r.farePerKm());
        return rules.save(rule);
    }

    public java.util.List<FareRule> listRules() {
        return rules.findAll();
    }
}
