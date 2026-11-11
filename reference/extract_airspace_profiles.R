#!/usr/bin/env Rscript
# ---------------------------------------------------------------------------
# Extract NM FIR entry/exit sequences (airspace profiles) for one day: the
# ground truth for OPDI's `xing-fir` events.
#
# Runs ONLY on the work laptop -- it needs ROracle plus PRU_READ_USR /
# PRU_READ_PWD / PRU_READ_DBNAME. Never run this from the pipeline, from a
# benchmark, or from a Quarto render.
#
#   Rscript reference/extract_airspace_profiles.R 2026-06-14
#   Rscript reference/extract_airspace_profiles.R 2026-06-14 CTFM
#
# WHAT IT WRITES
#
# reference/airspace_profiles_<day>_<profile>.parquet, one row per flight and
# FIR volume crossed, from eurocontrol::airspace_profiles_tidy():
#
#   ID, SEQ_ID, AIRSPACE_ID, AIRSPACE_TYPE, MODEL_TYPE,
#   ENTRY_TIME, ENTRY_LON, ENTRY_LAT, ENTRY_FL,
#   EXIT_TIME, EXIT_LON, EXIT_LAT, EXIT_FL
#
# joined to flights_tidy() for the columns that let it meet an ADS-B track:
# AIRCRAFT_ADDRESS (= icao24), CALLSIGN (AP_C_FLTID), ADEP, ADES.
#
# The profile matters. CPF is the correlated position report profile (radar
# based) and is the fairest comparator for an ADS-B derived crossing; CTFM is
# NM's own model of the flown trajectory. Default CPF. NM profiles exclude
# GA, State and military flights, so a flight OPDI sees with no truth row is
# not a miss.
#
# Scored by benchmarks/fir_truth_score.py against the `xing-fir` events of the
# same day: a crossing matches when the flight (icao24 + callsign + day), the
# volume pair and the time within 120 s agree.
# ---------------------------------------------------------------------------

OUT_DIR <- "reference"

suppressPackageStartupMessages({
  library(eurocontrol)
  library(arrow)
  library(dplyr)
  library(lubridate)
})

main <- function(day_arg, profile = "CPF") {
  day <- suppressWarnings(ymd(day_arg))
  if (is.na(day)) {
    stop("Could not parse '", day_arg, "'. Expected YYYY-MM-DD, e.g. 2026-06-14.")
  }
  # One day of departures plus a margin either side so a flight airborne at
  # midnight is whole; the scorer joins on the flight, not the day.
  wef <- format(as_datetime(day, tz = "UTC") - hours(12), "%Y-%m-%d %H:%M:%S")
  til <- format(as_datetime(day, tz = "UTC") + hours(36), "%Y-%m-%d %H:%M:%S")

  conn <- eurocontrol::db_connection("PRU_READ")
  on.exit(DBI::dbDisconnect(conn), add = TRUE)

  profiles <- airspace_profiles_tidy(conn = conn, wef = wef, til = til,
                                     airspace = "FIR", profile = profile)
  flights <- flights_tidy(conn = conn, wef = wef, til = til) |>
    select("ID", "AIRCRAFT_ADDRESS", "CALLSIGN", "ADEP", "ADES", "AOBT_3")

  out <- profiles |>
    inner_join(flights, by = "ID") |>
    collect() |>
    arrange(.data$ID, .data$SEQ_ID)

  path <- file.path(OUT_DIR, sprintf("airspace_profiles_%s_%s.parquet", format(day), profile))
  write_parquet(out, path)
  message(sprintf("wrote %s: %d rows, %d flights", path, nrow(out), n_distinct(out$ID)))
}

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) stop("usage: extract_airspace_profiles.R YYYY-MM-DD [CPF|CTFM]")
main(args[[1]], if (length(args) >= 2) args[[2]] else "CPF")
