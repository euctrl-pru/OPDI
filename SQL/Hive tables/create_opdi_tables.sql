CREATE TABLE project_opdi.opdi_flight_list (
    id STRING COMMENT 'Unique identifier for each flight in the table.',
    ADEP STRING COMMENT 'Airport of Departure.',
    ADES STRING COMMENT 'Airport of Destination.',
    ADEP_P STRING COMMENT 'Potential other Airport of Departure.',
    ADES_P STRING COMMENT 'Potential other Airport of Destination.',
    ICAO24 STRING COMMENT 'Unique ICAO 24-bit address of the transponder in hexadecimal string representation.',
    FLT_ID STRING COMMENT 'Flight trajectory identificator, the callsign.',
    first_seen TIMESTAMP COMMENT 'Timestamp indicating the time of the first observed ADS-B emission of the flight.',
    last_seen TIMESTAMP COMMENT 'Timestamp indicating the time of the last observed ADS-B emission of the flight.',
    DOF TIMESTAMP COMMENT 'Date of the flight (i.e., the date of it was first seen).',
    version STRING COMMENT 'Unique identifier for each row in the table.'
)
COMMENT 'A flight table constructed from OpenSky Netwerk (OSN) ADS-B statevectors with unique identifiers.'
STORED AS parquet
TBLPROPERTIES ('transactional'='false');


CREATE TABLE IF NOT EXISTS project_opdi.opdi_flight_list_clustered (
    id STRING COMMENT 'Unique identifier for each flight in the table.',
    ADEP STRING COMMENT 'Airport of Departure.',
    ADES STRING COMMENT 'Airport of Destination.',
    ADEP_P STRING COMMENT 'Potential other Airport of Departure.',
    ADES_P STRING COMMENT 'Potential other Airport of Destination.',
    ICAO24 STRING COMMENT 'Unique ICAO 24-bit address of the transponder in hexadecimal string representation.',
    FLT_ID STRING COMMENT 'Flight trajectory identificator, the callsign.',
    first_seen TIMESTAMP COMMENT 'Timestamp indicating the time of the first observed ADS-B emission of the flight.',
    last_seen TIMESTAMP COMMENT 'Timestamp indicating the time of the last observed ADS-B emission of the flight.',
    DOF TIMESTAMP COMMENT 'Date of the flight (i.e., the date it was first seen).',
    version STRING COMMENT 'Unique identifier for each row in the table.'
)
COMMENT 'A flight table constructed from OpenSky Network (OSN) ADS-B state vectors with unique identifiers.'
CLUSTERED BY (id, ADEP, ADES, ICAO24, FLT_ID, first_seen, last_seen, DOF)
INTO 1024 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('transactional'='false');

  
  CREATE TABLE IF NOT EXISTS `project_opdi`.`opdi_flight_events` (

      id STRING COMMENT 'Primary Key: Unique milestone identifier for each record.',
      flight_id STRING COMMENT 'Foreign Key: Identifier linking the flight event to the flight trajectory table.',

      type STRING COMMENT 'Type of the flight event (milestone) being recorded.',
      event_time TIMESTAMP COMMENT 'Timestamp for the flight event.',
      longitude DOUBLE COMMENT 'Longitude coordinate of the flight event.',
      latitude DOUBLE COMMENT 'Latitude coordinate of the flight event.',
      altitude DOUBLE COMMENT 'Altitude at which the flight event occurred.',
      
      source STRING COMMENT 'Source of the trajectory data.',
      version STRING COMMENT 'Version of the flight event (milestone) determination algorithm.',
      info STRING COMMENT 'Additional information or description of the flight event.'
  )
  COMMENT '`project_opdi`.`opdi_flight_events` table containing various OSN trajectory flight events. Last updated: 9/08/2024.'
  STORED AS parquet
  TBLPROPERTIES ('transactional'='false');
  
CREATE TABLE IF NOT EXISTS `project_opdi`.`opdi_flight_events_clustered` (
    id STRING COMMENT 'Primary Key: Unique milestone identifier for each record.',
    flight_id STRING COMMENT 'Foreign Key: Identifier linking the flight event to the flight trajectory table.',
    milestone_type STRING COMMENT 'Type of the flight event (milestone) being recorded.',
    event_time BIGINT COMMENT 'Timestamp for the flight event.',
    longitude DOUBLE COMMENT 'Longitude coordinate of the flight event.',
    latitude DOUBLE COMMENT 'Latitude coordinate of the flight event.',
    altitude DOUBLE COMMENT 'Altitude at which the flight event occurred.',
    source STRING COMMENT 'Source of the trajectory data.',
    version STRING COMMENT 'Version of the flight event (milestone) determination algorithm.',
    info STRING COMMENT 'Additional information or description of the flight event.'
)
COMMENT '`project_opdi`.`opdi_flight_events` table containing various OSN trajectory flight events. Last updated: 9/08/2024.'
CLUSTERED BY (id, flight_id, milestone_type, event_time, latitude, longitude, altitude)
INTO 2048 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('transactional'='false');


CREATE TABLE IF NOT EXISTS `project_opdi`.`opdi_measurements` (  
  id STRING COMMENT 'Primary Key: Unique measurement identifier for each record.', 
  milestone_id STRING COMMENT 'Foreign Key: Identifier linking the measurement to the corresponding event in the flight events table.',  
  type STRING COMMENT 'Type of measurement, e.g. flown distance or fuel consumption.', 
  value DOUBLE COMMENT 'Value of the measurement.', 
  version STRING COMMENT 'The version of the measurement calculation.' ) 
  COMMENT '`project_opdi`.opdi_measurements` table containing various measurements for OPDI flight events. Last updated: 9/8/2024.' 
  STORED AS parquet TBLPROPERTIES ('transactional'='false');
  
CREATE TABLE IF NOT EXISTS `project_opdi`.`opdi_measurements_clustered` (  
  id STRING COMMENT 'Primary Key: Unique measurement identifier for each record.', 
  milestone_id STRING COMMENT 'Foreign Key: Identifier linking the measurement to the corresponding event in the flight events table.',  
  type STRING COMMENT 'Type of measurement, e.g. flown distance or fuel consumption.', 
  value DOUBLE COMMENT 'Value of the measurement.', 
  version STRING COMMENT 'The version of the measurement calculation.' 
) 
COMMENT '`project_opdi`.opdi_measurements` table containing various measurements for OPDI flight events. Last updated: 9/8/2024.' 
CLUSTERED BY (id, milestone_id, type) 
INTO 1024 BUCKETS
STORED AS PARQUET 
TBLPROPERTIES ('transactional'='false');
