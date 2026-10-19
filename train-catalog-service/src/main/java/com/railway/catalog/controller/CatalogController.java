package com.railway.catalog.controller;

import com.railway.catalog.domain.Entities.CoachType;
import com.railway.catalog.domain.Entities.Route;
import com.railway.catalog.domain.Entities.Seat;
import com.railway.catalog.domain.Entities.Train;
import com.railway.catalog.domain.Entities.TrainCoach;
import com.railway.catalog.domain.Entities.TrainType;
import com.railway.catalog.repository.Repositories.CoachTypeRepository;
import com.railway.catalog.repository.Repositories.RouteRepository;
import com.railway.catalog.repository.Repositories.SeatRepository;
import com.railway.catalog.repository.Repositories.TrainCoachRepository;
import com.railway.catalog.repository.Repositories.TrainRepository;
import com.railway.catalog.repository.Repositories.TrainTypeRepository;
import com.railway.catalog.service.CatalogService;
import com.railway.catalog.service.Dtos.CoachRequest;
import com.railway.catalog.service.Dtos.JourneyInfo;
import com.railway.catalog.service.Dtos.RouteRequest;
import com.railway.catalog.service.Dtos.SeatInfo;
import com.railway.catalog.service.Dtos.TrainRequest;
import jakarta.validation.Valid;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/catalog")
@RequiredArgsConstructor
public class CatalogController {

    private final CatalogService service;
    private final TrainTypeRepository trainTypes;
    private final CoachTypeRepository coachTypes;
    private final TrainRepository trains;
    private final TrainCoachRepository coaches;
    private final SeatRepository seats;
    private final RouteRepository routes;

    @GetMapping("/train-types")
    public List<TrainType> listTrainTypes() { return trainTypes.findAll(); }

    @PostMapping("/train-types") @ResponseStatus(HttpStatus.CREATED)
    public TrainType createTrainType(@RequestBody TrainType t) { t.setId(null); return trainTypes.save(t); }

    @GetMapping("/coach-types")
    public List<CoachType> listCoachTypes() { return coachTypes.findAll(); }

    @PostMapping("/coach-types") @ResponseStatus(HttpStatus.CREATED)
    public CoachType createCoachType(@RequestBody CoachType c) { c.setId(null); return coachTypes.save(c); }

    @GetMapping("/trains")
    public List<Train> listTrains() { return trains.findAll(); }

    @GetMapping("/trains/{id}")
    public Train getTrain(@PathVariable("id") Long id) { return service.getTrain(id); }

    @PostMapping("/trains") @ResponseStatus(HttpStatus.CREATED)
    public Train createTrain(@Valid @RequestBody TrainRequest r) { return service.createTrain(r); }

    @GetMapping("/trains/{id}/coaches")
    public List<TrainCoach> listCoaches(@PathVariable("id") Long id) { return coaches.findByTrainId(id); }

    @PostMapping("/trains/{id}/coaches") @ResponseStatus(HttpStatus.CREATED)
    public TrainCoach addCoach(@PathVariable("id") Long id, @Valid @RequestBody CoachRequest r) {
        return service.addCoach(id, r);
    }

    @GetMapping("/coaches/{coachId}/seats")
    public List<Seat> listSeats(@PathVariable("coachId") Long coachId) {
        return seats.findByCoachIdOrderBySeatNumber(coachId);
    }

    @GetMapping("/seats/{seatId}")
    public SeatInfo seatInfo(@PathVariable("seatId") Long seatId) { return service.seatInfo(seatId); }

    @GetMapping("/trains/{id}/routes")
    public List<Route> listRoutes(@PathVariable("id") Long id) { return routes.findByTrainIdOrderByStopSequence(id); }

    @PostMapping("/trains/{id}/routes") @ResponseStatus(HttpStatus.CREATED)
    public Route addRoute(@PathVariable("id") Long id, @Valid @RequestBody RouteRequest r) {
        return service.addRoute(id, r);
    }

    @GetMapping("/trains/{id}/journey-info")
    public JourneyInfo journeyInfo(@PathVariable("id") Long id, @RequestParam("from") String from,
                                   @RequestParam("to") String to) {
        return service.journeyInfo(id, from, to);
    }
}
