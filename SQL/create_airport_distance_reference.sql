CREATE TABLE airport_distance_reference (
  lat DOUBLE COMMENT 'Latitude of the grid point.',
  lon DOUBLE COMMENT 'Longitude of the grid point.',
  airport_ident STRING COMMENT 'Identifier of the airport ICAO.',
  distance DOUBLE COMMENT 'Distance from the grid point to the airport in kilometers.'
)
COMMENT 'Grids for airports.'
STORED AS parquet
TBLPROPERTIES ('transactional'='false');
