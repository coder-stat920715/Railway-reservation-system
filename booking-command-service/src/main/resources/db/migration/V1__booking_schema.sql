CREATE TABLE bookings (
    booking_id      UUID PRIMARY KEY,
    pnr             VARCHAR(10)    NOT NULL UNIQUE,
    status          VARCHAR(30)    NOT NULL,
    total_amount    NUMERIC(12, 2) NOT NULL,
    train_id        BIGINT         NOT NULL,
    seat_id         BIGINT         NOT NULL,
    coach_type_id   BIGINT         NOT NULL,
    from_station    VARCHAR(10)    NOT NULL,
    to_station      VARCHAR(10)    NOT NULL,
    journey_date    DATE           NOT NULL,
    passenger_name  VARCHAR(120)   NOT NULL,
    passenger_email VARCHAR(160),
    distance_km     NUMERIC(8, 2)  NOT NULL,
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    version         BIGINT         NOT NULL DEFAULT 0
);

-- A seat can be held by at most one non-cancelled booking per train and journey date.
CREATE UNIQUE INDEX uq_active_seat ON bookings (train_id, journey_date, seat_id) WHERE status <> 'CANCELLED';

-- Transactional outbox: rows are inserted in the same transaction as the domain change and captured by Debezium.
CREATE TABLE outbox_events (
    id             UUID PRIMARY KEY,
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id   VARCHAR(100) NOT NULL,
    event_type     VARCHAR(100) NOT NULL,
    payload        JSONB        NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    processed      BOOLEAN      NOT NULL DEFAULT FALSE
);
CREATE INDEX idx_outbox_created_at ON outbox_events (created_at);

CREATE TABLE saga_instances (
    saga_id        UUID PRIMARY KEY,
    booking_id     UUID         NOT NULL UNIQUE REFERENCES bookings (booking_id),
    state          VARCHAR(30)  NOT NULL,
    failure_reason VARCHAR(255),
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Consumer idempotency: an event id can be applied only once.
CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
