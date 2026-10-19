package com.railway.fare.repository;

import com.railway.fare.domain.FareRule;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface FareRuleRepository extends JpaRepository<FareRule, Long> {
    Optional<FareRule> findByTrainTypeIdAndCoachTypeId(Long trainTypeId, Long coachTypeId);
}
