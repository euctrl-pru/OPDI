CREATE TABLE oa_airport_frequencies (
  id INT COMMENT 'Internal OurAirports integer identifier for the frequency. This will stay persistent, even if the radio frequency or description changes.',
  airport_ref INT COMMENT 'Internal integer foreign key matching the id column for the associated airport in oa_airports.',
  airport_ident STRING COMMENT 'Externally-visible string foreign key matching the ident column for the associated airport in oa_airports.',
  type STRING COMMENT 'A code for the frequency type. Some common values are "TWR" (tower), "ATF" or "CTAF" (common traffic frequency), "GND" (ground control), "RMP" (ramp control), "ATIS" (automated weather), "RCO" (remote radio outlet), "ARR" (arrivals), "DEP" (departures), "UNICOM" (monitored ground station), and "RDO" (a flight-service station).',
  description STRING COMMENT 'A description of the frequency, typically the way a pilot would open a call on it.',
  frequency_mhz DOUBLE COMMENT 'Radio voice frequency in megahertz. Note that the same frequency may appear multiple times for an airport, serving different functions.'
)
COMMENT 'OurAirports airport frequencies data (for PRU).'
STORED AS parquet
TBLPROPERTIES ('transactional'='false');
