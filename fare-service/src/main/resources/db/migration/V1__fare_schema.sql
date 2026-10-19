CREATE TABLE fare_rules (
    id            BIGSERIAL PRIMARY KEY,
    train_type_id BIGINT         NOT NULL,
    coach_type_id BIGINT         NOT NULL,
    constant_fare NUMERIC(10, 2) NOT NULL CHECK (constant_fare >= 0),
    fare_per_km   NUMERIC(8, 4)  NOT NULL CHECK (fare_per_km >= 0),
    UNIQUE (train_type_id, coach_type_id)
);

-- Seed: train types 1..3 (EXPRESS, SUPERFAST, RAJDHANI) x coach types 1..3 (SLEEPER, AC3, AC2)
INSERT INTO fare_rules (train_type_id, coach_type_id, constant_fare, fare_per_km)
SELECT t, c, 50 + t * 10 + c * 20, 0.5 + c * 0.4
FROM generate_series(1, 3) AS t, generate_series(1, 3) AS c;
