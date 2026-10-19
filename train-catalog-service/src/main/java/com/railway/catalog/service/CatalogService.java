package com.railway.catalog.service;

import com.railway.catalog.domain.Entities.CoachType;
import com.railway.catalog.domain.Entities.Route;
import com.railway.catalog.domain.Entities.Seat;
import com.railway.catalog.domain.Entities.Train;
import com.railway.catalog.domain.Entities.TrainCoach;
import com.railway.catalog.repository.Repositories.CoachTypeRepository;
import com.railway.catalog.repository.Repositories.RouteRepository;
import com.railway.catalog.repository.Repositories.SeatRepository;
import com.railway.catalog.repository.Repositories.TrainCoachRepository;
import com.railway.catalog.repository.Repositories.TrainRepository;
import com.railway.catalog.repository.Repositories.TrainTypeRepository;
import com.railway.catalog.service.Dtos.CoachRequest;
import com.railway.catalog.service.Dtos.JourneyInfo;
import com.railway.catalog.service.Dtos.RouteRequest;
import com.railway.catalog.service.Dtos.SeatInfo;
import com.railway.catalog.service.Dtos.TrainRequest;
import java.util.List;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
@RequiredArgsConstructor
public class CatalogService {

    private final TrainTypeRepository trainTypes;
    private final CoachTypeRepository coachTypes;
    private final TrainRepository trains;
    private final TrainCoachRepository coaches;
    private final SeatRepository seats;
    private final RouteRepository routes;

    @Transactional
    public Train createTrain(TrainRequest r) {
        trainTypes.findById(r.trainTypeId()).orElseThrow(() -> notFound("Train type " + r.trainTypeId()));
        Train t = new Train();
        t.setTrainNumber(r.trainNumber());
        t.setName(r.name());
        t.setTrainTypeId(r.trainTypeId());
        return trains.save(t);
    }

    public Train getTrain(Long id) {
        return trains.findById(id).orElseThrow(() -> notFound("Train " + id));
    }

    /** Adds a coach to a train and generates its seats from the coach type capacity. */
    @Transactional
    public TrainCoach addCoach(Long trainId, CoachRequest r) {
        getTrain(trainId);
        CoachType type = coachTypes.findById(r.coachTypeId())
                .orElseThrow(() -> notFound("Coach type " + r.coachTypeId()));
        TrainCoach coach = new TrainCoach();
        coach.setTrainId(trainId);
        coach.setCoachTypeId(type.getId());
        coach.setCoachNumber(r.coachNumber());
        coach = coaches.save(coach);

        final Long coachId = coach.getId();
        List<Seat> generated = IntStream.rangeClosed(1, type.getSeatCapacity()).mapToObj(n -> {
            Seat s = new Seat();
            s.setCoachId(coachId);
            s.setSeatNumber(n);
            return s;
        }).toList();
        seats.saveAll(generated);
        return coach;
    }

    @Transactional
    public Route addRoute(Long trainId, RouteRequest r) {
        getTrain(trainId);
        Route route = new Route();
        route.setTrainId(trainId);
        route.setStopSequence(r.stopSequence());
        route.setStationCode(r.stationCode());
        route.setStationName(r.stationName());
        route.setDistanceFromOriginKm(r.distanceFromOriginKm());
        route.setArrivalTime(r.arrivalTime());
        route.setDepartureTime(r.departureTime());
        return routes.save(route);
    }

    public JourneyInfo journeyInfo(Long trainId, String from, String to) {
        Train train = getTrain(trainId);
        Route a = routes.findByTrainIdAndStationCode(trainId, from)
                .orElseThrow(() -> notFound("Station " + from + " on train " + trainId));
        Route b = routes.findByTrainIdAndStationCode(trainId, to)
                .orElseThrow(() -> notFound("Station " + to + " on train " + trainId));
        if (b.getStopSequence() <= a.getStopSequence()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Destination must come after origin on the route");
        }
        return new JourneyInfo(trainId, train.getTrainTypeId(), from, to,
                b.getDistanceFromOriginKm().subtract(a.getDistanceFromOriginKm()));
    }

    public SeatInfo seatInfo(Long seatId) {
        Seat seat = seats.findById(seatId).orElseThrow(() -> notFound("Seat " + seatId));
        TrainCoach coach = coaches.findById(seat.getCoachId()).orElseThrow(() -> notFound("Coach " + seat.getCoachId()));
        return new SeatInfo(seat.getId(), seat.getSeatNumber(), coach.getId(), coach.getCoachTypeId(), coach.getTrainId());
    }

    private static ResponseStatusException notFound(String what) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, what + " not found");
    }
}
