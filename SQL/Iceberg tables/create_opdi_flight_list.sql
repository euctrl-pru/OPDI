CREATE TABLE IF NOT EXISTS project_opdi.opdi_flight_list (
    ID STRING COMMENT 'Unique identifier for each flight in the table.',
    ICAO24 STRING COMMENT 'Unique ICAO 24-bit address of the transponder in hexadecimal string representation.',
    FLT_ID STRING COMMENT 'Flight trajectory identificator, the callsign.',
    DOF TIMESTAMP COMMENT 'Date of the flight (i.e., the date it was first seen).',
    ADEP STRING COMMENT 'Airport of Departure.',
    ADES STRING COMMENT 'Airport of Destination.',
    ADEP_P STRING COMMENT 'Potential other Airport of Departure.',
    ADES_P STRING COMMENT 'Potential other Airport of Destination.',
    REGISTRATION STRING COMMENT 'This column contains the aircraft registration, commonly referred to as the "tail number," which uniquely identifies the aircraft within its country of origin.',
    MODEL STRING COMMENT 'This column specifies the aircraft model as defined by the manufacturer (e.g., "Boeing 737-800").',
    TYPECODE STRING COMMENT 'This column represents the aircraft type designator as per ICAO, providing a standard abbreviation for the model (e.g., "B738" for Boeing 737-800).',
    ICAO_AIRCRAFT_CLASS STRING COMMENT 'This column contains the ICAO-defined aircraft type category, classifying the aircraft by usage (e.g., "L2J" for light aircraft with two jet engines).',
    ICAO_OPERATOR STRING COMMENT 'This column specifies the ICAO operator designator, which identifies the airline or organization operating the aircraft.',
    FIRST_SEEN TIMESTAMP COMMENT 'Timestamp indicating the time of the first observed ADS-B emission of the flight.',
    LAST_SEEN TIMESTAMP COMMENT 'Timestamp indicating the time of the last observed ADS-B emission of the flight.',
    VERSION STRING COMMENT 'Version of the OPDI flight table.'
)
COMMENT 'The OPDI flight table constructed from OpenSky Network (OSN) ADS-B state vectors with unique identifiers and extended with aircraft metadata.'
PARTITIONED BY SPEC(DAY(DOF))
STORED BY ICEBERG
TBLPROPERTIES (
    'write.sort.order' = 'ID ASC, ICAO24 ASC, FLT_ID ASC, ICAO_AIRCRAFT_CLASS ASC',
    'write.distribution-mode' = 'hash',
    'parquet.compression' = 'SNAPPY',
    'bucketing_version' = '2',
    'engine.hive.enabled' = 'true',
    'format-version' = '2',
    'numFiles' = '0',
    'numFilesErasureCoded' = '0',
    'numRows' = '0',
    'rawDataSize' = '0',
    'serialization.format' = '1',
    'table_type' = 'ICEBERG',
    'totalSize' = '0',
    'write.delete.mode' = 'merge-on-read',
    'write.distribution.mode' = 'hash',
    'write.merge.mode' = 'merge-on-read',
    'write.parquet.compression-codec' = 'SNAPPY',
    'write.update.mode' = 'merge-on-read'
);
