-- One logical database per service (database-per-service pattern) on a single Postgres server.
CREATE DATABASE catalog_db;
CREATE DATABASE fare_db;
CREATE DATABASE booking_command_db;
CREATE DATABASE payment_db;
CREATE DATABASE booking_query_db;
