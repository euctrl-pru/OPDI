CREATE TABLE IF NOT EXISTS `project_opdi`.`opdi_measurements` (
    id STRING COMMENT 'Primary Key: Unique measurement identifier for each record.',
    milestone_id STRING COMMENT 'Foreign Key: Identifier linking the measurement to the corresponding event in the flight events table.',
    type STRING COMMENT 'Type of measurement, e.g. flown distance or fuel consumption.',
    value DOUBLE COMMENT 'Value of the measurement.',
    version STRING COMMENT 'The version of the measurement calculation.'
)
COMMENT '`project_opdi`.`opdi_measurements` table containing various measurements for OPDI flight events.'
PARTITIONED BY SPEC(type, version)
STORED BY ICEBERG
TBLPROPERTIES (
    'write.sort.order' = 'id ASC, milestone_id ASC',
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
