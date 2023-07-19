CREATE TABLE oa_runways (
  id INT COMMENT 'Internal OurAirports integer identifier for the runway. This will stay persistent, even if the runway numbering changes.',
  airport_ref INT COMMENT 'Internal integer foreign key matching the id column for the associated airport in ao_airports.',
  airport_ident STRING COMMENT 'Externally-visible string foreign key matching the ident column for the associated airport in ao_airports.',
  length_ft INT COMMENT 'Length of the full runway surface (including displaced thresholds, overrun areas, etc) in feet.',
  width_ft INT COMMENT 'Width of the runway surface in feet.',
  surface STRING COMMENT 'Code for the runway surface type. Some common values include "ASP" (asphalt), "TURF" (turf), "CON" (concrete), "GRS" (grass), "GRE" (gravel), "WATER" (water), and "UNK" (unknown).',
  lighted BOOLEAN COMMENT '1 if the surface is lighted at night, 0 otherwise.',
  closed BOOLEAN COMMENT '1 if the runway surface is currently closed, 0 otherwise.',
  le_ident STRING COMMENT 'Identifier for the low-numbered end of the runway.',
  le_latitude_deg DOUBLE COMMENT 'Latitude of the centre of the low-numbered end of the runway, in decimal degrees (positive is north), if available.',
  le_longitude_deg DOUBLE COMMENT 'Longitude of the centre of the low-numbered end of the runway, in decimal degrees (positive is east), if available.',
  le_elevation_ft INT COMMENT 'Elevation above MSL of the low-numbered end of the runway in feet.',
  le_heading_degT DOUBLE COMMENT 'Heading of the low-numbered end of the runway in degrees true (not magnetic).',
  le_displaced_threshold_ft INT COMMENT 'Length of the displaced threshold (if any) for the low-numbered end of the runway, in feet.',
  he_ident STRING COMMENT 'Identifier for the high-numbered end of the runway.',
  he_latitude_deg DOUBLE COMMENT 'Latitude of the centre of the high-numbered end of the runway, in decimal degrees (positive is north), if available.',
  he_longitude_deg DOUBLE COMMENT 'Longitude of the centre of the high-numbered end of the runway, in decimal degrees (positive is east), if available.',
  he_elevation_ft INT COMMENT 'Elevation above MSL of the high-numbered end of the runway in feet.',
  he_heading_degT DOUBLE COMMENT 'Heading of the high-numbered end of the runway in degrees true (not magnetic).',
  he_displaced_threshold_ft INT COMMENT 'Length of the displaced threshold (if any) for the high-numbered end of the runway, in feet.'
)
COMMENT 'OurAirports airport runways data (for PRU) - Monthly updated.'
STORED AS parquet
TBLPROPERTIES ('transactional'='false');
