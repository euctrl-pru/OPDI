CREATE TABLE oa_airports (
  id INT COMMENT 'Internal OurAirports integer identifier for the airport. This will stay persistent, even if the airport code changes.',
  ident STRING COMMENT 'The text identifier used in the OurAirports URL. This will be the ICAO code if available. Otherwise, it will be a local airport code (if no conflict), or if nothing else is available, an internally-generated code starting with the ISO2 country code, followed by a dash and a four-digit number.',
  type STRING COMMENT 'The type of the airport. Allowed values are "closed_airport", "heliport", "large_airport", "medium_airport", "seaplane_base", and "small_airport".',
  name STRING COMMENT 'The official airport name, including "Airport", "Airstrip", etc.',
  latitude_deg DOUBLE COMMENT 'The airport latitude in decimal degrees (positive for north).',
  longitude_deg DOUBLE COMMENT 'The airport longitude in decimal degrees (positive for east).',
  elevation_ft INT COMMENT 'The airport elevation MSL in feet (not metres).',
  continent STRING COMMENT 'The code for the continent where the airport is (primarily) located.',
  iso_country STRING COMMENT 'The two-character ISO 3166:1-alpha2 code for the country where the airport is (primarily) located.',
  iso_region STRING COMMENT 'An alphanumeric code for the high-level administrative subdivision of a country where the airport is primarily located (e.g. province, governorate), prefixed by the ISO2 country code and a hyphen.',
  municipality STRING COMMENT 'The primary municipality that the airport serves (when available).',
  scheduled_service STRING COMMENT '"yes" if the airport currently has scheduled airline service; "no" otherwise.',
  gps_code STRING COMMENT 'The code that an aviation GPS database (such as Jeppesen''s or Garmin''s) would normally use for the airport. This will always be the ICAO code if one exists.',
  iata_code STRING COMMENT 'The three-letter IATA code for the airport (if it has one).',
  local_code STRING COMMENT 'The local country code for the airport, if different from the gps_code and iata_code fields (used mainly for US airports).',
  home_link STRING COMMENT 'URL of the airport''s official home page on the web, if one exists.',
  wikipedia_link STRING COMMENT 'URL of the airport''s page on Wikipedia, if one exists.',
  keywords STRING COMMENT 'Extra keywords/phrases to assist with search, comma-separated. May include former names for the airport, alternate codes, names in other languages, nearby tourist destinations, etc.'
)
COMMENT 'OurAirports airports data (for PRU) - Monthly updated.'
STORED AS parquet
TBLPROPERTIES ('transactional'='false');
