-- ============================================================================
-- OPDI Table Creation Script - LOCAL Environment
-- Database: opdi_local
-- Warehouse: ./data/warehouse
--
-- Run this script in Spark SQL before the first pipeline execution.
-- All statements use IF NOT EXISTS so they are safe to re-run.
--
-- Note: Uses Spark SQL syntax (USING iceberg) rather than CDP Hive syntax
-- (STORED BY ICEBERG) since local runs use Spark directly.
-- ============================================================================


-- ============================================================================
-- 1. Ingestion Tables
-- ============================================================================

-- 1a. OpenSky Network State Vectors
CREATE TABLE IF NOT EXISTS `opdi_local`.`osn_statevectors_v2` (
    event_time TIMESTAMP COMMENT 'This column contains the timestamp for which the state vector was valid.',
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
    baro_altitude DOUBLE COMMENT 'This column indicates the aircrafts altitude. Barometric altitude measured by the barometer (in meter).',
    geo_altitude DOUBLE COMMENT 'This column indicates the aircrafts altitude determined using the GNSS (GPS) sensor (in meter).',
    last_pos_update DOUBLE COMMENT 'This unix timestamp indicates the age of the position.',
    last_contact DOUBLE COMMENT 'This unix timestamp indicates the time at which OpenSky received the last signal of the aircraft.',
    serials ARRAY<INT> COMMENT 'The serials column is a list of serials of the ADS-B receivers which received the message.'
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
    'format-version'='2',
    'write.parquet.compression-codec'='SNAPPY',
    'write.delete.mode'='merge-on-read',
    'write.distribution.mode'='hash',
    'write.merge.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
)
COMMENT 'OpenSky Network state vectors - Weekly updated.';


-- 1b. OpenSky Network Aircraft Database
CREATE TABLE IF NOT EXISTS `opdi_local`.`osn_aircraft_db` (
    icao24 STRING COMMENT 'This column contains the 24-bit ICAO transponder ID, which is unique to each airframe and used to identify the aircraft.',
    registration STRING COMMENT 'This column contains the aircraft registration, commonly referred to as the "tail number," which uniquely identifies the aircraft within its country of origin.',
    model STRING COMMENT 'This column specifies the aircraft model as defined by the manufacturer (e.g., "Boeing 737-800").',
    typecode STRING COMMENT 'This column represents the aircraft type designator as per ICAO, providing a standard abbreviation for the model (e.g., "B738" for Boeing 737-800).',
    icao_aircraft_class STRING COMMENT 'This column contains the ICAO-defined aircraft type category, classifying the aircraft by usage (e.g., "L2J" for light aircraft with two jet engines).',
    icao_operator STRING COMMENT 'This column specifies the ICAO operator designator, which identifies the airline or organization operating the aircraft.'
)
USING iceberg
TBLPROPERTIES (
    'format-version'='2',
    'write.sort.order'='icao24 ASC, registration ASC, icao_operator ASC',
    'write.parquet.compression-codec'='SNAPPY',
    'write.delete.mode'='merge-on-read',
    'write.distribution.mode'='hash',
    'write.merge.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
)
COMMENT '`opdi_local`.`osn_aircraft_db` holds detailed metadata about aircraft including identifiers, registrations, and classifications. The data originates from the OpenSky Network Aircraft Database.';


-- 1c. OurAirports - Airports
CREATE TABLE IF NOT EXISTS `opdi_local`.`oa_airports` (
    id INT, ident STRING, type STRING, name STRING,
    latitude_deg DOUBLE, longitude_deg DOUBLE, elevation_ft INT,
    continent STRING, iso_country STRING, iso_region STRING,
    municipality STRING, scheduled_service STRING,
    gps_code STRING, iata_code STRING, local_code STRING,
    home_link STRING, wikipedia_link STRING, keywords STRING
)
COMMENT 'OurAirports airports data.'
STORED AS parquet TBLPROPERTIES ('transactional'='false');


-- 1d. OurAirports - Runways
CREATE TABLE IF NOT EXISTS `opdi_local`.`oa_runways` (
    id INT, airport_ref INT, airport_ident STRING,
    length_ft INT, width_ft INT, surface STRING,
    lighted BOOLEAN, closed BOOLEAN,
    le_ident STRING, le_latitude_deg DOUBLE, le_longitude_deg DOUBLE,
    le_elevation_ft INT, le_heading_degT DOUBLE, le_displaced_threshold_ft INT,
    he_ident STRING, he_latitude_deg DOUBLE, he_longitude_deg DOUBLE,
    he_elevation_ft INT, he_heading_degT DOUBLE, he_displaced_threshold_ft INT
)
COMMENT 'OurAirports runways data.'
STORED AS parquet TBLPROPERTIES ('transactional'='false');


-- 1e. OurAirports - Navaids
CREATE TABLE IF NOT EXISTS `opdi_local`.`oa_navaids` (
    id INT, filename STRING, ident STRING, name STRING, type STRING,
    frequency_khz INT, latitude_deg DOUBLE, longitude_deg DOUBLE,
    elevation_ft INT, iso_country STRING,
    dme_frequency_khz INT, dme_channel STRING,
    dme_latitude_deg DOUBLE, dme_longitude_deg DOUBLE, dme_elevation_ft INT,
    slaved_variation_deg DOUBLE, magnetic_variation_deg DOUBLE,
    usageType STRING, power STRING, associated_airport STRING
)
COMMENT 'OurAirports navaids data.'
STORED AS parquet TBLPROPERTIES ('transactional'='false');


-- 1f. OurAirports - Airport Frequencies
CREATE TABLE IF NOT EXISTS `opdi_local`.`oa_airport_frequencies` (
    id INT, airport_ref INT, airport_ident STRING,
    type STRING, description STRING, frequency_mhz DOUBLE
)
COMMENT 'OurAirports airport frequencies.'
STORED AS parquet TBLPROPERTIES ('transactional'='false');


-- 1g. OurAirports - Countries
CREATE TABLE IF NOT EXISTS `opdi_local`.`oa_countries` (
    id INT, code STRING, name STRING, continent STRING,
    wikipedia_link STRING, keywords STRING
)
COMMENT 'OurAirports countries data.'
STORED AS parquet TBLPROPERTIES ('transactional'='false');


-- 1h. OurAirports - Regions
CREATE TABLE IF NOT EXISTS `opdi_local`.`oa_regions` (
    id INT, code STRING, local_code STRING, name STRING,
    continent STRING, iso_country STRING,
    wikipedia_link STRING, keywords STRING
)
COMMENT 'OurAirports regions data.'
STORED AS parquet TBLPROPERTIES ('transactional'='false');


-- ============================================================================
-- 2. Pipeline Tables
-- ============================================================================

-- 2a. Flight Tracks
CREATE TABLE IF NOT EXISTS `opdi_local`.`osn_tracks` (
    event_time TIMESTAMP COMMENT 'This column contains the timestamp for which the state vector was valid.',
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
    baro_altitude DOUBLE COMMENT 'This column indicates the aircraft altitude. Barometric altitude measured in meters.',
    geo_altitude DOUBLE COMMENT 'This column indicates the aircraft altitude based on GNSS (GPS) sensors in meters.',
    last_pos_update DOUBLE COMMENT 'This Unix timestamp indicates the age of the position.',
    last_contact DOUBLE COMMENT 'This Unix timestamp indicates the time at which OpenSky received the last signal of the aircraft.',
    serials ARRAY<INT> COMMENT 'The serials column is a list of serials of the ADS-B receivers which received the message.',
    track_id STRING COMMENT 'Unique identifier for the associated flight track.',
    h3_res_7 STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution 7.',
    h3_res_12 STRING COMMENT 'H3 cell identifier for lat and lon with H3 resolution 12.',
    segment_distance_nm DOUBLE COMMENT 'The distance from the previous state vector in nautical miles.',
    cumulative_distance_nm DOUBLE COMMENT 'The cumulative distance from the start in nautical miles.',
    geo_altitude_c DOUBLE COMMENT 'This column indicates the cleaned GNSS-measured altitude in meters.',
    baro_altitude_c DOUBLE COMMENT 'This column indicates the cleaned barometric altitude in meters.'
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
    'format-version'='2',
    'write.sort.order'='event_time ASC, baro_altitude ASC, track_id ASC, h3_res_7 ASC, h3_res_12 ASC',
    'write.parquet.compression-codec'='SNAPPY',
    'write.delete.mode'='merge-on-read',
    'write.distribution.mode'='hash',
    'write.merge.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
)
COMMENT '`opdi_local`.`osn_statevectors_v2` with added track_ids (generated based on callsign, icao24 grouping with 30 min signal gap intolerance) and H3 tags.';


-- 2b. Flight List
CREATE TABLE IF NOT EXISTS `opdi_local`.`opdi_flight_list_v2` (
    ID STRING COMMENT 'Unique identifier for each flight in the table.',
    ICAO24 STRING COMMENT 'Unique ICAO 24-bit address of the transponder in hexadecimal string representation.',
    FLT_ID STRING COMMENT 'Flight trajectory identificator, the callsign.',
    DOF DATE COMMENT 'Date of the flight (i.e., the date it was first seen).',
    ADEP STRING COMMENT 'Airport of Departure (ICAO code).',
    ADES STRING COMMENT 'Airport of Destination (ICAO code).',
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
USING iceberg
PARTITIONED BY (days(FIRST_SEEN))
TBLPROPERTIES (
    'format-version'='2',
    'write.sort.order'='ID ASC, ICAO24 ASC, FLT_ID ASC, ICAO_AIRCRAFT_CLASS ASC',
    'write.parquet.compression-codec'='SNAPPY',
    'write.delete.mode'='merge-on-read',
    'write.distribution.mode'='hash',
    'write.merge.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
)
COMMENT 'The OPDI flight table v2 constructed from OpenSky Network (OSN) ADS-B state vectors with unique identifiers and extended with aircraft metadata.';


-- 2c. Flight Events
CREATE TABLE IF NOT EXISTS `opdi_local`.`opdi_flight_events` (
    id STRING COMMENT 'Primary Key: Unique milestone identifier for each record.',
    flight_id STRING COMMENT 'Foreign Key: Identifier linking the flight event to the flight trajectory table.',
    type STRING COMMENT 'Type of the flight event (milestone) being recorded.',
    event_time TIMESTAMP COMMENT 'Timestamp for the flight event.',
    longitude DOUBLE COMMENT 'Longitude coordinate of the flight event.',
    latitude DOUBLE COMMENT 'Latitude coordinate of the flight event.',
    altitude DOUBLE COMMENT 'Altitude at which the flight event occurred (in feet).',
    source STRING COMMENT 'Source of the trajectory data.',
    version STRING COMMENT 'Version of the flight event (milestone) determination algorithm.',
    info STRING COMMENT 'Additional information or description of the flight event (JSON for airport events).'
)
USING iceberg
PARTITIONED BY (type, version)
TBLPROPERTIES (
    'format-version'='2',
    'write.sort.order'='id ASC, flight_id ASC, event_time ASC',
    'write.parquet.compression-codec'='SNAPPY',
    'write.delete.mode'='merge-on-read',
    'write.distribution.mode'='hash',
    'write.merge.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
)
COMMENT '`opdi_local`.`opdi_flight_events` table containing various OSN trajectory flight events.';


-- 2d. Measurements
CREATE TABLE IF NOT EXISTS `opdi_local`.`opdi_measurements` (
    id STRING COMMENT 'Primary Key: Unique measurement identifier for each record.',
    milestone_id STRING COMMENT 'Foreign Key: Identifier linking the measurement to the corresponding event in the flight events table.',
    type STRING COMMENT 'Type of measurement, e.g. flown distance or fuel consumption.',
    value DOUBLE COMMENT 'Value of the measurement.',
    version STRING COMMENT 'The version of the measurement calculation.'
)
USING iceberg
PARTITIONED BY (type, version)
TBLPROPERTIES (
    'format-version'='2',
    'write.sort.order'='id ASC, milestone_id ASC',
    'write.parquet.compression-codec'='SNAPPY',
    'write.delete.mode'='merge-on-read',
    'write.distribution.mode'='hash',
    'write.merge.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
)
COMMENT '`opdi_local`.`opdi_measurements` table containing various measurements for OPDI flight events.';


-- ============================================================================
-- 3. Reference Tables
-- ============================================================================

-- 3a. H3 Airport Layouts
CREATE TABLE IF NOT EXISTS `opdi_local`.`hexaero_airport_layouts` (
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
USING iceberg
PARTITIONED BY (hexaero_apt_icao)
TBLPROPERTIES (
    'format-version'='2',
    'write.sort.order'='hexaero_h3_id ASC, hexaero_res ASC, hexaero_type ASC, hexaero_latitude ASC, hexaero_longitude ASC',
    'write.parquet.compression-codec'='SNAPPY',
    'write.delete.mode'='merge-on-read',
    'write.distribution.mode'='hash',
    'write.merge.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
)
COMMENT 'Hexaero Airport Layouts Iceberg Table. It contains the hex_ids following the Uber H3 system for relevant airport elements as extracted from OpenStreetMap (OSM).';


-- 3b. H3 Airspace Reference
CREATE TABLE IF NOT EXISTS `opdi_local`.`opdi_h3_airspace_ref` (
    airspace_type STRING COMMENT 'Type of airspace (e.g., flight information region (FIR), Air Navigation Service Provider (ANSP), country (COUNTRY)).',
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
USING iceberg
PARTITIONED BY (airspace_type)
TBLPROPERTIES (
    'format-version'='2',
    'write.sort.order'='validity_start ASC, h3_res_7 ASC, min_fl ASC, max_fl ASC',
    'write.parquet.compression-codec'='SNAPPY',
    'write.delete.mode'='merge-on-read',
    'write.distribution.mode'='hash',
    'write.merge.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
)
COMMENT 'The OPDI H3 airspace reference table defining airspace structures with validity periods and flight levels. It lists all H3 indexes per airspace.';
