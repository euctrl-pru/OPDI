CREATE TABLE IF NOT EXISTS project_opdi.opdi_h3_airspace_ref (
    airspace_type STRING COMMENT 'Type of airspace (e.g., flight information region (FIR), Air Navigation Service Provider (ANSP), country (COUNTRY)),...',
    name STRING COMMENT 'The name or designation of the airspace.',
    code STRING COMMENT 'A (non necessarily unique) identifier or code assigned to the airspace.',
    airac_cfmu STRING COMMENT 'The AIRAC cycle and CFMU reference indicating the airspace definition update.',
    validity_start TIMESTAMP COMMENT 'Timestamp indicating the start date and time when this airspace definition is valid.',
    validity_end TIMESTAMP COMMENT 'Timestamp indicating the end date and time when this airspace definition expires.',
    min_fl INT COMMENT 'The minimum flight level at which the airspace applies.',
    max_fl INT COMMENT 'The maximum flight level at which the airspace applies.',
    h3_res_7 STRING COMMENT 'All H3 indexes at res 7 for each airspace (exploded).',
    h3_res_7_lat DOUBLE COMMENT 'The latitude of the center of the associated h3_res_7 index.',
    h3_res_7_lon DOUBLE COMMENT 'The longitude of the center of the associated h3_res_7 index.'
)
COMMENT 'The OPDI H3 airspace reference table defining airspace structures with validity periods and flight levels. It lists all H3 indexes per airspace.'
PARTITIONED BY SPEC(airspace_type)
STORED BY ICEBERG
TBLPROPERTIES (
    'write.sort.order' = 'validity_start ASC, h3_res_7 ASC, min_fl ASC, max_fl ASC',
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
