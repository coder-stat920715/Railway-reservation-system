package com.railway.booking.client;

import com.railway.booking.client.RemoteDtos.FareRequest;
import com.railway.booking.client.RemoteDtos.FareResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@FeignClient(name = "fare-service", path = "/api/fares")
public interface FareClient {

    @PostMapping("/calculate")
    FareResponse calculate(@RequestBody FareRequest request);
}
