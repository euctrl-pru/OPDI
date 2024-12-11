CREATE TABLE IF NOT EXISTS `project_aiu`.`osn_h3_statevectors_bucket_sample` (
        event_time BIGINT COMMENT 'This column contains the unix (aka POSIX or epoch) timestamp for which the state vector was valid.',
        icao24 STRING COMMENT 'This column contains the 24-bit ICAO transponder ID which can be used to track specific airframes over different flights.',
        lat DOUBLE COMMENT 'This column contains the last known latitude of the aircraft.',
        lon DOUBLE COMMENT 'This column contains the last known longitude of the aircraft.',
        velocity DOUBLE COMMENT 'This column contains the speed over ground of the aircraft in meters per second.',
        heading DOUBLE COMMENT 'This column represents the direction of movement (track angle in degrees) as the clockwise angle from the geographic north.',
        vert_rate DOUBLE COMMENT 'This column contains the vertical speed of the aircraft in meters per second.',
        callsign STRING COMMENT 'This column contains the callsign that was broadcast by the aircraft.',
        on_ground BOOLEAN COMMENT 'This flag indicates whether the aircraft is broadcasting surface positions (true) or airborne positions (false).',
        alert BOOLEAN COMMENT 'This flag is a special indicator used in ATC.',
        spi BOOLEAN COMMENT 'This flag is a special indicator used in ATC.',
        squawk STRING COMMENT 'This 4-digit octal number is another transponder code which is used by ATC and pilots for identification purposes and indication of emergencies.',
        baro_altitude DOUBLE COMMENT 'This column indicates the aircrafts altitude. As the names suggest, baroaltitude is the altitude measured by the barometer (in meter).',
        geo_altitude DOUBLE COMMENT 'This column indicates the aircrafts altitude. As the names suggest, geoaltitude is determined using the GNSS (GPS) sensor (in meter).',
        last_pos_update DOUBLE COMMENT 'This unix timestamp indicates the age of the position.',
        last_contact DOUBLE COMMENT 'This unix timestamp indicates the time at which OpenSky received the last signal of the aircraft.',
        serials ARRAY<INT> COMMENT 'The serials column is a list of serials of the ADS-B receivers which received the message.',
        h3_res_12 STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution 12.',
        h3_res_11 STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution 11.',
        h3_res_10 STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution 10.',
        h3_res_9 STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution 9.',
        h3_res_8 STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution 8.',
        h3_res_7 STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution 7.',
        h3_res_6 STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution 6.',
        h3_res_5 STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution 5.',
        h3_res_4 STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution 4.',
        h3_res_3 STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution 3.',
        h3_res_2 STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution 2.',
        h3_res_1 STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution 1.',
        h3_res_0 STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution 0.'
    )
    COMMENT '`project_aiu`.`osn_h3_statevectors_bucket_sample` with added H3 tags (generated based on callsign, icao24). Last updated: 29 April 2024.'
    CLUSTERED BY (icao24, callsign, event_time, h3_res_11) INTO 1024 BUCKETS
    STORED AS parquet
    TBLPROPERTIES ('transactional'='false');