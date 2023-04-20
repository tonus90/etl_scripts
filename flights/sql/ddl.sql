-- create table opensky with flights
CREATE TABLE if not exists opensky
(
    callsign VARCHAR(255),
    number VARCHAR(255),
    icao24 VARCHAR(255),
    registration VARCHAR(255),
    typecode VARCHAR(255),
    origin VARCHAR(255),
    destination VARCHAR(255),
    firstseen TIMESTAMP,
    lastseen TIMESTAMP,
    day TIMESTAMP,
    latitude_1 DOUBLE PRECISION,
    longitude_1 DOUBLE PRECISION,
    altitude_1 DOUBLE PRECISION,
    latitude_2 DOUBLE PRECISION,
    longitude_2 DOUBLE PRECISION,
    altitude_2 DOUBLE PRECISION
)