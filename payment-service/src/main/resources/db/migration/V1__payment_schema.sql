CREATE TABLE payments (
    payment_id     UUID PRIMARY KEY,
    booking_id     UUID           NOT NULL UNIQUE,
    amount         NUMERIC(12, 2) NOT NULL,
    status         VARCHAR(20)    NOT NULL,
    failure_reason VARCHAR(255),
    created_at     TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ    NOT NULL DEFAULT now()
);

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

CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
