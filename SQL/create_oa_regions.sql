CREATE TABLE oa_regions (
  id INT COMMENT 'Internal OurAirports integer identifier for the region. This will stay persistent, even if the region code changes.',
  code STRING COMMENT 'Local_code prefixed with the country code to make a globally-unique identifier.',
  local_code STRING COMMENT 'The local code for the administrative subdivision.',
  name STRING COMMENT 'The common English-language name for the administrative subdivision.',
  continent STRING COMMENT 'A code for the continent to which the region belongs.',
  iso_country STRING COMMENT 'The two-character ISO 3166:1-alpha2 code for the country containing the administrative subdivision.',
  wikipedia_link STRING COMMENT 'A link to the Wikipedia article describing the subdivision.',
  keywords STRING COMMENT 'A comma-separated list of keywords to assist with search. May include former names for the region, and/or the region name in other languages.'
)
COMMENT 'OurAirports airport regions data (for PRU) - Monthly updated.'
STORED AS parquet
TBLPROPERTIES ('transactional'='false');
