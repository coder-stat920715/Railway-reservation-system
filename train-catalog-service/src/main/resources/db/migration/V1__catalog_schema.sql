CREATE TABLE train_types (
    id          BIGSERIAL PRIMARY KEY,
    name        VARCHAR(60) NOT NULL UNIQUE,
    description VARCHAR(255)
);

CREATE TABLE coach_types (
    id            BIGSERIAL PRIMARY KEY,
    name          VARCHAR(60) NOT NULL UNIQUE,
    seat_capacity INT NOT NULL CHECK (seat_capacity > 0)
);

CREATE TABLE trains (
    id            BIGSERIAL PRIMARY KEY,
    train_number  VARCHAR(10)  NOT NULL UNIQUE,
    name          VARCHAR(120) NOT NULL,
    train_type_id BIGINT NOT NULL REFERENCES train_types (id)
);

CREATE TABLE train_coaches (
    id            BIGSERIAL PRIMARY KEY,
    train_id      BIGINT NOT NULL REFERENCES trains (id),
    coach_type_id BIGINT NOT NULL REFERENCES coach_types (id),
    coach_number  VARCHAR(10) NOT NULL,
    UNIQUE (train_id, coach_number)
);

CREATE TABLE seats (
    id          BIGSERIAL PRIMARY KEY,
    coach_id    BIGINT NOT NULL REFERENCES train_coaches (id),
    seat_number INT NOT NULL,
    UNIQUE (coach_id, seat_number)
);

CREATE TABLE routes (
    id                      BIGSERIAL PRIMARY KEY,
    train_id                BIGINT NOT NULL REFERENCES trains (id),
    stop_sequence           INT NOT NULL,
    station_code            VARCHAR(10)  NOT NULL,
    station_name            VARCHAR(100) NOT NULL,
    distance_from_origin_km NUMERIC(8, 2) NOT NULL,
    arrival_time            TIME,
    departure_time          TIME,
    UNIQUE (train_id, stop_sequence),
    UNIQUE (train_id, station_code)
);
