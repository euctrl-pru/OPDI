CREATE TABLE project_aiu.osn_flight_table_with_id (
    ICAO24 STRING COMMENT 'Unique ICAO 24-bit address of the transponder in hexadecimal string representation.',
    FLT_ID STRING COMMENT 'Flight trajectory identificator, the callsign.',
    ADEP STRING COMMENT 'Airport of Departure.',
    ADEP_MIN_DISTANCE_KM FLOAT COMMENT 'Minimum distance the ADS-B signal is picked up from the airport of departure in kilometers.',
    ADEP_MIN_DISTANCE_TIME TIMESTAMP COMMENT 'Timestamp at which the minimum distance from ADEP was recorded.',
    ADES STRING COMMENT 'Airport of Destination.',
    ADES_MIN_DISTANCE_KM FLOAT COMMENT 'Minimum distance the ADS-B signal is picked up from the airport of destination in kilometers.',
    ADES_MIN_DISTANCE_TIME TIMESTAMP COMMENT 'Timestamp at which the minimum distance from ADES was recorded.',
    unique_id BIGINT COMMENT 'Unique identifier for each row in the table.'
)
COMMENT 'A flight table constructed from OpenSky Netwerk (OSN) ADS-B statevectors with unique identifiers.'
STORED AS parquet
TBLPROPERTIES ('transactional'='false');


CREATE TABLE project_opdi.osn_flight_table (
    id STRING COMMENT 'Unique identifier for each flight in the table.'
    ADEP STRING COMMENT 'Airport of Departure.',
    ADES STRING COMMENT 'Airport of Destination.',
    ADEP_P STRING COMMENT 'Potential other Airport of Departure.',
    ADES_P STRING COMMENT 'Potential other Airport of Destination.',
    ICAO24 STRING COMMENT 'Unique ICAO 24-bit address of the transponder in hexadecimal string representation.',
    FLT_ID STRING COMMENT 'Flight trajectory identificator, the callsign.',
    first_seen TIMESTAMP COMMENT 'Timestamp indicating the time of the first observed ADS-B emission of the flight.',
    last_seen TIMESTAMP COMMENT 'Timestamp indicating the time of the last observed ADS-B emission of the flight.',
    DOF TIMESTAMP COMMENT 'Date of the flight (i.e., the date of it was first seen).'
    version STRING COMMENT 'Unique identifier for each row in the table.'
)
COMMENT 'A flight table constructed from OpenSky Netwerk (OSN) ADS-B statevectors with unique identifiers.'
STORED AS parquet
TBLPROPERTIES ('transactional'='false');
