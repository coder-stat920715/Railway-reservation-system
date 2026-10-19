package com.railway.catalog.repository;

import com.railway.catalog.domain.Entities.CoachType;
import com.railway.catalog.domain.Entities.Route;
import com.railway.catalog.domain.Entities.Seat;
import com.railway.catalog.domain.Entities.Train;
import com.railway.catalog.domain.Entities.TrainCoach;
import com.railway.catalog.domain.Entities.TrainType;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public final class Repositories {
    private Repositories() {}

    public interface TrainTypeRepository extends JpaRepository<TrainType, Long> {}

    public interface CoachTypeRepository extends JpaRepository<CoachType, Long> {}

    public interface TrainRepository extends JpaRepository<Train, Long> {}

    public interface TrainCoachRepository extends JpaRepository<TrainCoach, Long> {
        List<TrainCoach> findByTrainId(Long trainId);
    }

    public interface SeatRepository extends JpaRepository<Seat, Long> {
        List<Seat> findByCoachIdOrderBySeatNumber(Long coachId);
    }

    public interface RouteRepository extends JpaRepository<Route, Long> {
        List<Route> findByTrainIdOrderByStopSequence(Long trainId);

        Optional<Route> findByTrainIdAndStationCode(Long trainId, String stationCode);
    }
}
