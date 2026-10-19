package com.railway.booking.client;

import com.railway.booking.client.RemoteDtos.JourneyInfo;
import com.railway.booking.client.RemoteDtos.SeatInfo;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(name = "train-catalog-service", path = "/api/catalog")
public interface CatalogClient {

    @GetMapping("/trains/{trainId}/journey-info")
    JourneyInfo journeyInfo(@PathVariable("trainId") Long trainId, @RequestParam("from") String from,
                            @RequestParam("to") String to);

    @GetMapping("/seats/{seatId}")
    SeatInfo seat(@PathVariable("seatId") Long seatId);
}
