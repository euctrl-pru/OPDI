CREATE TABLE project_aiu.opdi_runway_hexagons_bucketed_europe (
    AIRPORT_IDENT STRING COMMENT 'ICAO24 identifier for the airport.',
    LE_IDENT STRING COMMENT 'Identifier for the lower end of the runway.',
    LE_HEADING_DEG FLOAT COMMENT 'Heading in degrees of the lower end of the runway.',
    HE_IDENT STRING COMMENT 'Identifier for the higher end of the runway.',
    HE_HEADING_DEG FLOAT COMMENT 'Heading in degrees of the higher end of the runway.',
    HEX_ID STRING COMMENT 'Unique identifier for the hexagon in H3 string representation.',
    HEX_RES INT COMMENT 'H3 resolution of HEX_ID.',
    HEX_LON FLOAT COMMENT 'Longitude of the hexagon center.',
    HEX_LAT FLOAT COMMENT 'Latitude of the hexagon center.',
    GATE_ID STRING COMMENT 'Type of gate identifier.',
    GATE_ID_NR INT COMMENT 'Numerical identifier for the gate or segment.'
)
CLUSTERED BY (HEX_ID, HEX_LON, HEX_LAT) INTO 1024 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ('transactional'='false');