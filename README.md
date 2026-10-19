# Railway Reservation System

Spring Boot 3.3 / Java 21 / Spring Cloud 2023.0.x microservices with SAGA orchestration, Transactional Outbox,
Debezium CDC -> Kafka, and CQRS.

## Run
```
docker compose up --build
```
Gateway: http://localhost:8080 - Eureka: http://localhost:8761 - Kafka Connect: http://localhost:8089

## Flow
```
POST /api/bookings -> bookings(CREATED->PAYMENT_PENDING) + outbox rows (one ACID tx)
  -> Debezium -> booking-events -> payment-service -> payment outbox -> payment-events
  -> booking-command SAGA -> CONFIRMED  (or CANCELLED on PaymentFailed)
  -> all events projected by booking-query-service (read model)
```

## Try it
```
curl localhost:8080/api/catalog/trains
curl localhost:8080/api/catalog/coaches/1/seats
curl -X POST localhost:8080/api/fares/calculate -H 'Content-Type: application/json' \
  -d '{"trainTypeId":3,"coachTypeId":2,"distanceKm":1384}'
curl -X POST localhost:8080/api/bookings -H 'Content-Type: application/json' \
  -d '{"trainId":1,"seatId":1,"fromStation":"MMCT","toStation":"NDLS","journeyDate":"2027-01-15","passengerName":"Asha Rao","passengerEmail":"asha@example.com"}'
curl localhost:8080/api/query/bookings/<bookingId>
```
A passenger name starting with `FAIL` makes the simulated payment gateway decline, which triggers SAGA compensation.
