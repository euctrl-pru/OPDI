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
PARTITIONED BY SPEC(type, version)
STORED BY ICEBERG
TBLPROPERTIES (
    'write.sort.order' = 'id ASC, flight_id ASC, event_time ASC',
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
