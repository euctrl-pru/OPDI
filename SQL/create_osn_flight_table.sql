CREATE TABLE project_aiu.osn_flight_table (
    ICAO24 STRING COMMENT 'Unique ICAO 24-bit address of the transponder in hexadecimal string representation.',
    FLT_ID STRING COMMENT 'Flight trajectory identificator, the callsign.',
    ADEP STRING COMMENT 'Airport of Departure.',
    ADEP_MIN_DISTANCE_KM FLOAT COMMENT 'Minimum distance the ADS-B signal is picked up from the airport of departure in kilometers.',
    ADEP_MIN_DISTANCE_TIME TIMESTAMP COMMENT 'Timestamp at which the minimum distance from ADEP was recorded.',
    ADES STRING COMMENT 'Airport of Destination.',
    ADES_MIN_DISTANCE_KM FLOAT COMMENT 'Minimum distance the ADS-B signal is picked up from the airport of destination in kilometers.',
    ADES_MIN_DISTANCE_TIME TIMESTAMP COMMENT 'Timestamp at which the minimum distance from ADES was recorded.'
)
COMMENT 'A flight table constructed from OpenSky Netwerk (OSN) ADS-B statevectors.'
STORED AS parquet
TBLPROPERTIES ('transactional'='false');
