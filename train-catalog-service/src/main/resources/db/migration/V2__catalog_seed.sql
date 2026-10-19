INSERT INTO train_types (name, description) VALUES
    ('EXPRESS', 'Express service'),
    ('SUPERFAST', 'Superfast service'),
    ('RAJDHANI', 'Premium long-distance service');

INSERT INTO coach_types (name, seat_capacity) VALUES
    ('SLEEPER', 72),
    ('AC3', 64),
    ('AC2', 48);

INSERT INTO trains (train_number, name, train_type_id)
SELECT '12951', 'Mumbai Rajdhani', id FROM train_types WHERE name = 'RAJDHANI';
INSERT INTO trains (train_number, name, train_type_id)
SELECT '12009', 'Shatabdi Express', id FROM train_types WHERE name = 'SUPERFAST';

INSERT INTO train_coaches (train_id, coach_type_id, coach_number)
SELECT t.id, c.id, v.num
FROM trains t
JOIN (VALUES ('A1', 'AC3'), ('B1', 'AC2'), ('S1', 'SLEEPER')) AS v (num, ctype) ON TRUE
JOIN coach_types c ON c.name = v.ctype
WHERE t.train_number = '12951'
ORDER BY v.num;

INSERT INTO seats (coach_id, seat_number)
SELECT tc.id, g
FROM train_coaches tc
JOIN coach_types ct ON ct.id = tc.coach_type_id,
     generate_series(1, ct.seat_capacity) AS g
ORDER BY tc.id, g;

INSERT INTO routes (train_id, stop_sequence, station_code, station_name, distance_from_origin_km, arrival_time, departure_time)
SELECT t.id, r.seq, r.code, r.sname, r.dist, r.arr::time, r.dep::time
FROM trains t,
     (VALUES (1, 'MMCT', 'Mumbai Central', 0,    '16:25', '16:25'),
             (2, 'BRC',  'Vadodara',       392,  '20:00', '20:10'),
             (3, 'RTM',  'Ratlam',         655,  '22:25', '22:30'),
             (4, 'KOTA', 'Kota',           919,  '00:55', '01:00'),
             (5, 'NDLS', 'New Delhi',      1384, '08:15', '08:15')) AS r (seq, code, sname, dist, arr, dep)
WHERE t.train_number = '12951';
