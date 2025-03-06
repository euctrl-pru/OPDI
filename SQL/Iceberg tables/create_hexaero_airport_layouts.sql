CREATE TABLE project_opdi.hexaero_airport_layouts (
    hexaero_apt_icao STRING COMMENT 'This column contains the ICAO code of the airport.',
    hexaero_h3_id STRING COMMENT 'This column contains a unique identifier for the h3 tag.',
    hexaero_latitude DOUBLE COMMENT 'This column contains the latitude of the h3 tag center.',
    hexaero_longitude DOUBLE COMMENT 'This column contains the longitude of the h3 tag center.',
    hexaero_res INT COMMENT 'This column contains the h3 resolution.',
    hexaero_aeroway STRING COMMENT 'This column describes the type of aeroway.',
    hexaero_length DOUBLE COMMENT 'This column contains the length of the aeroway in meters.',
    hexaero_ref STRING COMMENT 'This column contains a reference identifier for the aeroway from OpenStreetMap (OSM).',
    hexaero_surface STRING COMMENT 'This column describes the surface type of the aeroway.',
    hexaero_width DOUBLE COMMENT 'This column contains the width of the aeroway in meters.',
    hexaero_osm_id BIGINT COMMENT 'This column contains a unique identifier for the osm element that this tag comprises.',
    hexaero_type STRING COMMENT 'This column describes the OSM type of the OSM element.'
)
COMMENT 'Hexaero Airport Layouts Iceberg Table. It contains the hex_ids following the Uber H3 system for relevant airport elements as extracted from OpenStreetMap (OSM).'
PARTITIONED BY SPEC (hexaero_apt_icao)
STORED BY ICEBERG
TBLPROPERTIES (
    'write.sort.order' = 'hexaero_h3_id ASC, hexaero_res ASC, hexaero_type ASC, hexaero_latitude ASC, hexaero_longitude ASC',
    'write.distribution-mode' = 'hash',
    'bucketing_version'='2',
    'engine.hive.enabled'='true',
    'format-version'='2',
    'numFiles'='0',
    'numFilesErasureCoded'='0',
    'numRows'='0',
    'parquet.compression'='SNAPPY',
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