-- Denormalised, read-optimised projection of bookings + payments (CQRS read model).
CREATE TABLE booking_view (
    booking_id          UUID PRIMARY KEY,
    pnr                 VARCHAR(10),
    status              VARCHAR(30),
    total_amount        NUMERIC(12, 2),
    train_id            BIGINT,
    seat_id             BIGINT,
    from_station        VARCHAR(10),
    to_station          VARCHAR(10),
    journey_date        DATE,
    passenger_name      VARCHAR(120),
    payment_status      VARCHAR(20),
    cancellation_reason VARCHAR(255),
    created_at          TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_view_pnr ON booking_view (pnr);
CREATE INDEX idx_view_passenger ON booking_view (lower(passenger_name));
CREATE INDEX idx_view_status ON booking_view (status);

CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
