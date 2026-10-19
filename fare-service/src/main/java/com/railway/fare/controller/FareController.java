package com.railway.fare.controller;

import com.railway.fare.domain.FareRule;
import com.railway.fare.service.FareDtos.FareRequest;
import com.railway.fare.service.FareDtos.FareResponse;
import com.railway.fare.service.FareDtos.FareRuleRequest;
import com.railway.fare.service.FareService;
import jakarta.validation.Valid;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/fares")
@RequiredArgsConstructor
public class FareController {

    private final FareService service;

    @PostMapping("/calculate")
    public FareResponse calculate(@Valid @RequestBody FareRequest request) {
        return service.calculate(request);
    }

    @GetMapping("/rules")
    public List<FareRule> rules() {
        return service.listRules();
    }

    @PostMapping("/rules")
    public FareRule upsert(@Valid @RequestBody FareRuleRequest request) {
        return service.upsert(request);
    }
}
