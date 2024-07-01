CREATE TABLE oa_countries (
  id INT COMMENT 'Internal OurAirports integer identifier for the country. This will stay persistent, even if the country name or code changes.',
  code STRING COMMENT 'The two-character ISO 3166:1-alpha2 code for the country.',
  name STRING COMMENT 'The common English-language name for the country.',
  continent STRING COMMENT 'The code for the continent where the country is (primarily) located.',
  wikipedia_link STRING COMMENT 'Link to the Wikipedia article about the country.',
  keywords STRING COMMENT 'A comma-separated list of search keywords/phrases related to the country.'
)
COMMENT 'OurAirports airport countries data (for PRU) - Monthly updated.'
STORED AS parquet
TBLPROPERTIES ('transactional'='false');
