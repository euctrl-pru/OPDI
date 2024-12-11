CREATE TABLE project_aiu.airport_proximity_flight_data--_clustered (
  event_time BIGINT COMMENT 'Timestamp of the event',
  icao24 STRING COMMENT '24-bit ICAO transponder ID to track specific airframes over different flights.',
  lat DOUBLE COMMENT 'Latitude of the flight, rounded to 3 decimal places.',
  lon DOUBLE COMMENT 'Longitude of the flight, rounded to 3 decimal places.',
  velocity DOUBLE COMMENT 'Velocity of the flight',
  heading DOUBLE COMMENT 'Heading of the flight',
  vert_rate DOUBLE COMMENT 'Vertical rate of the flight',
  callsign STRING COMMENT 'Callsign of the flight',
  on_ground BOOLEAN COMMENT 'Whether the flight is on the ground',
  baro_altitude DOUBLE COMMENT 'Aircraft altitude measured by the barometer (in meter).',
  airport_ident STRING COMMENT 'Identifier of the airport ICAO.',
  ident STRING COMMENT 'The text identifier used in the OurAirports URL, which is the ICAO code.',
  elevation_ft INT COMMENT 'The airport elevation MSL in feet.',
  height_above_airport DOUBLE COMMENT 'Aircraft height above the airport, computed as (baro_altitude - elevation_ft).',
  distance DOUBLE COMMENT 'Distance to the airport',
  track_id STRING COMMENT 'Unique identifier for the associated flight tracks in osn_flight_table_with_id'
)
--CLUSTERED BY (event_time, icao24, callsign, track_id) INTO 2048 BUCKETS
COMMENT 'Merged table containing flight data below 10000 feet near airports.'
STORED AS parquet
TBLPROPERTIES ('transactional'='false');
