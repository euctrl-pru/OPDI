CREATE TABLE oa_navaids (
  id INT COMMENT 'Internal OurAirports integer identifier for the navaid. This will stay persistent, even if the navaid identifier or frequency changes.',
  filename STRING COMMENT 'Unique string identifier constructed from the navaid name and country, and used in the OurAirports URL.',
  ident STRING COMMENT 'The 1-3 character identifer that the navaid transmits.',
  name STRING COMMENT 'The name of the navaid, excluding its type.',
  type STRING COMMENT 'The type of the navaid.',
  frequency_khz INT COMMENT 'The frequency of the navaid in kilohertz.',
  latitude_deg DOUBLE COMMENT 'The latitude of the navaid in decimal degrees (negative for south).',
  longitude_deg DOUBLE COMMENT 'The longitude of the navaid in decimal degrees (negative for west).',
  elevation_ft INT COMMENT 'The navaids elevation MSL in feet (not metres).',
  iso_country STRING COMMENT 'The two-character ISO 3166:1-alpha2 code for the country that operates the navaid.',
  dme_frequency_khz INT COMMENT 'The paired VHF frequency for the DME (or TACAN) in kilohertz.',
  dme_channel STRING COMMENT 'The DME channel (an alternative way of tuning distance-measuring equipment).',
  dme_latitude_deg DOUBLE COMMENT 'The latitude of the associated DME in decimal degrees (negative for south). If missing, assume that the value is the same as latitude_deg.',
  dme_longitude_deg DOUBLE COMMENT 'The longitude of the associated DME in decimal degrees (negative for west). If missing, assume that the value is the same as longitude_deg.',
  dme_elevation_ft INT COMMENT 'The associated DME transmitters elevation MSL in feet. If missing, assume that its the same value as elevation_ft.',
  slaved_variation_deg DOUBLE COMMENT 'The magnetic variation adjustment built into a VORs, VOR-DMEs, or TACANs radials.',
  magnetic_variation_deg DOUBLE COMMENT 'The actual magnetic variation at the navaids location.',
  usageType STRING COMMENT 'The primary function of the navaid in the airspace system.',
  power STRING COMMENT 'The power-output level of the navaid.',
  associated_airport STRING COMMENT 'The OurAirports text identifier (usually the ICAO code) for an airport associated with the navaid.'
)
COMMENT 'OurAirports airport naivaids data (for PRU) - Monthly updated.'
STORED AS parquet
TBLPROPERTIES ('transactional'='false');
