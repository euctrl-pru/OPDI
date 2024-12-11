DROP TABLE `project_opdi`.`hexaero_airport_layouts_clustered`; 
DROP TABLE `project_opdi`.`hexaero_airport_layouts`; 

CREATE TABLE IF NOT EXISTS `project_opdi`.`hexaero_airport_layouts_clustered` (
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
    hexaero_osm_id INT COMMENT 'This column contains a unique identifier for the osm element that this tag comprises.',
    hexaero_type STRING COMMENT 'This column describes the OSM type of the OSM element.',
    hexaero_avg_heading DOUBLE COMMENT 'This column contains the average heading direction of the road.'
)
COMMENT '`project_opdi`.`hexaero_airport_layouts_clustered` with detailed aeroway and hexaero information. Last updated: 1 August 2024.'
CLUSTERED BY (hexaero_h3_id, hexaero_apt_icao) INTO 256 BUCKETS
STORED AS parquet
TBLPROPERTIES ('transactional'='false');

CREATE TABLE IF NOT EXISTS `project_opdi`.`hexaero_airport_layouts` (
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
    hexaero_osm_id INT COMMENT 'This column contains a unique identifier for the osm element that this tag comprises.',
    hexaero_type STRING COMMENT 'This column describes the OSM type of the OSM element.',
    hexaero_avg_heading DOUBLE COMMENT 'This column contains the average heading direction of the road.'
)
COMMENT '`project_opdi`.`hexaero_airport_layouts` with detailed aeroway and hexaero information. Last updated: 1 August 2024.'
--CLUSTERED BY (hexaero_h3_id, hexaero_apt_icao) INTO 256 BUCKETS
STORED AS parquet
TBLPROPERTIES ('transactional'='false');


