CREATE TABLE IF NOT EXISTS `project_opdi`.`osn_aircraft_db` (
    icao24 STRING COMMENT 'This column contains the 24-bit ICAO transponder ID, which is unique to each airframe and used to identify the aircraft.',
    registration STRING COMMENT 'This column contains the aircraft registration, commonly referred to as the "tail number," which uniquely identifies the aircraft within its country of origin.',
    model STRING COMMENT 'This column specifies the aircraft model as defined by the manufacturer (e.g., "Boeing 737-800").',
    typecode STRING COMMENT 'This column represents the aircraft type designator as per ICAO, providing a standard abbreviation for the model (e.g., "B738" for Boeing 737-800).',
    icao_aircraft_class STRING COMMENT 'This column contains the ICAO-defined aircraft type category, classifying the aircraft by usage (e.g., "L2J" for light aircraft with two jet engines).',
    icao_operator STRING COMMENT 'This column specifies the ICAO operator designator, which identifies the airline or organization operating the aircraft.'
)
COMMENT '`project_opdi`.`osn_aircraft_db` holds detailed metadata about aircraft including identifiers, registrations, and classifications. The data originates from the OpenSky Network Aircraft Database and is created through an open-source community collaboration.'
PARTITIONED BY SPEC(icao_aircrafttype)
STORED BY ICEBERG
TBLPROPERTIES (
    'write.sort.order' = 'icao24 ASC, registration ASC, icao_operator ASC',
    'write.distribution-mode' = 'hash',
    'parquet.compression' = 'SNAPPY',
    'bucketing_version'='2',
    'engine.hive.enabled'='true',
    'format-version'='2',
    'numFiles'='0',
    'numFilesErasureCoded'='0',
    'numRows'='0',
    'rawDataSize'='0',
    'serialization.format'='1',
    'table_type'='ICEBERG',
    'totalSize'='0',
    'write.delete.mode'='merge-on-read',
    'write.distribution.mode'='hash',
    'write.merge.mode'='merge-on-read',
    'write.parquet.compression-codec'='SNAPPY',
    'write.update.mode'='merge-on-read'
);