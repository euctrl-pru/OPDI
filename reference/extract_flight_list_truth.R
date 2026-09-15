#!/usr/bin/env Rscript
# ---------------------------------------------------------------------------
# APDF ground truth for the events_v0.3.0 flight list, in flight shape.
#
# Runs ONLY on the work laptop -- it needs ROracle plus PRU_READ_USR /
# PRU_READ_PWD / PRU_READ_DBNAME. Never run this from the pipeline, from a
# benchmark, or from a Quarto render: `quarto render` must succeed with no
# credentials and no database.
#
#   Rscript reference/extract_flight_list_truth.R                    # the campaign fortnight
#   Rscript reference/extract_flight_list_truth.R 2026-06-01 2026-06-14
#
# Both dates are INCLUSIVE.
#
# WHY THIS EXISTS BESIDE extract.R
#
#   extract.R pulls APDF in the shape the database returns it: long/movement
#   form, one row per (flight, phase). `opdi_flight_list` under events_v0.3.0
#   is flight-shaped -- one row per flight, with ATOT, ALDT, AOBT, AIBT, the
#   two runways, the two stands and the ring crossings side by side. Comparing
#   the two therefore begins with a pivot, and doing that pivot once, here,
#   beats doing it differently in every analysis that needs it.
#
#   This script also writes the raw monthly extracts, with the same filenames
#   and the same queries extract.R uses, so it is a superset rather than a
#   competing source of truth.
#
# WHAT APDF CAN AND CANNOT CHECK
#
#   Checkable:   ATOT ALDT AOBT AIBT  RWY_DEP RWY_ARR  STND_DEP STND_ARR
#                C40_DEP C40_ARR  C100_DEP C100_ARR
#   NOT checkable: C50 C60 C110 C120 -- APDF records crossings at 40 NM and
#                100 NM only. Those four flight-list columns have no ground
#                truth at all and must not be reported as validated.
#
# See reference/README.md for the schema of each extract, and MANIFEST.md for
# what to record afterwards.
# ---------------------------------------------------------------------------

DEFAULT_FROM <- "2026-06-01"   # the events_v0.3.0 acceptance campaign
DEFAULT_TO   <- "2026-06-14"
OUT_DIR <- "reference"

suppressPackageStartupMessages({
  library(eurocontrol)
  library(arrow)
  library(dplyr)
  library(lubridate)
})


collect_one <- function(label, query) {
  message("    ", label, " ...")
  # collect() is required: these are lazy Oracle-backed tables.
  df <- collect(query)
  message("      ", format(nrow(df), big.mark = ","), " rows")
  df
}


#' Every calendar month the window touches.
#'
#' The window is pulled a month at a time even when it fits inside one, and
#' that is a correctness requirement rather than tidiness. `apdf_tidy()`
#' filters on `MVT_TIME_UTC` *and* on `SRC_DATE_FROM` against the same window
#' (airport_operator_data_flow.R:109-114). APDF is delivered monthly, so
#' `SRC_DATE_FROM` tracks the delivery month: ask for 2026-06-05..2026-06-14
#' directly and every movement whose source record starts on 2026-06-01 --
#' which is all of them -- fails the second predicate and is dropped. No error,
#' no warning, an empty answer that looks like a quiet month.
months_spanned <- function(from, to) {
  start <- floor_date(from, "month")
  out <- c()
  while (start <= to) {
    out <- c(out, format(start, "%Y-%m-%d"))
    start <- start %m+% months(1)
  }
  as_date(out)
}


#' APDF's long form pivoted to one row per flight.
#'
#' `SRC_PHASE` is what decides which milestone a movement row carries: there is
#' no literal AOBT or ATOT column in APDF. DEP rows give the off-block and
#' take-off, ARR rows the landing and in-block, and the runway, stand and ring
#' columns mean the departure's or the arrival's depending on the same flag.
pivot_to_flight_shape <- function(apdf) {
  keyed <- apdf |> filter(!is.na(.data$ID))
  dropped <- nrow(apdf) - nrow(keyed)
  if (dropped > 0L) {
    message(sprintf(
      "    %s movement row(s) (%.2f%%) have no ID and cannot be joined -- dropped.",
      format(dropped, big.mark = ","), 100 * dropped / nrow(apdf)
    ))
  }

  dep <- keyed |>
    filter(.data$SRC_PHASE == "DEP") |>
    transmute(
      ID = .data$ID,
      ADEP = .data$ADEP_ICAO,
      ADES = .data$ADES_ICAO,
      FLTID = .data$AP_C_FLTID,
      REG = .data$AP_C_REG,
      ARCTYP = .data$ARCTYP,
      AOBT = .data$BLOCK_TIME_UTC,
      ATOT = .data$MVT_TIME_UTC,
      RWY_DEP = .data$AP_C_RWY,
      STND_DEP = .data$AP_C_STND,
      C40_DEP = .data$C40_CROSS_TIME,
      C100_DEP = .data$C100_CROSS_TIME
    )

  arr <- keyed |>
    filter(.data$SRC_PHASE == "ARR") |>
    transmute(
      ID = .data$ID,
      AIBT = .data$BLOCK_TIME_UTC,
      ALDT = .data$MVT_TIME_UTC,
      RWY_ARR = .data$AP_C_RWY,
      STND_ARR = .data$AP_C_STND,
      C40_ARR = .data$C40_CROSS_TIME,
      C100_ARR = .data$C100_CROSS_TIME
    )

  # A duplicated ID within one phase would fan the join out silently, turning
  # one flight into several and inflating every count computed downstream.
  # Check before joining, not after -- afterwards the evidence is gone.
  for (nm in c("dep", "arr")) {
    d <- get(nm)
    dup <- sum(duplicated(d$ID))
    if (dup > 0L) {
      warning(sprintf(
        "%s side has %s duplicated ID(s); the join will fan out. Investigate before using.",
        toupper(nm), format(dup, big.mark = ",")
      ), immediate. = TRUE)
    }
  }

  # full_join, not inner: a flight seen at only one end is a real flight and a
  # real test of the pipeline. An inner join would drop exactly the cases where
  # one side is missing, which is the thing coverage is trying to measure.
  full_join(dep, arr, by = "ID")
}


main <- function(from_arg, to_arg) {
  from <- suppressWarnings(ymd(from_arg))
  to <- suppressWarnings(ymd(to_arg))
  if (is.na(from) || is.na(to)) {
    stop("Could not parse '", from_arg, "' / '", to_arg, "'. Expected YYYY-MM-DD.")
  }
  if (to < from) stop("The end date precedes the start date.")
  if (!dir.exists(OUT_DIR)) {
    stop("Directory '", OUT_DIR, "' not found. Run this from the repo root.")
  }

  months <- months_spanned(from, to)
  message(sprintf(
    "Window %s .. %s inclusive, pulled as %d calendar month(s): %s",
    from, to, length(months), paste(format(months, "%Y-%m"), collapse = ", ")
  ))

  # One connection for every query, closed however this exits. on.exit only
  # fires inside a function -- hence main().
  conn <- db_connection(schema = "PRU_READ")
  on.exit({
    message("Closing DB connection.")
    try(DBI::dbDisconnect(conn), silent = TRUE)
  }, add = TRUE)

  apdf_all <- list()
  flights_all <- list()

  for (m in months) {
    m <- as_date(m)
    wef <- format(m, "%Y-%m-%d")
    til <- format(m %m+% months(1), "%Y-%m-%d")
    tag <- format(m, "%Y%m")
    message("  ", wef, " -> ", til, " (exclusive)")

    apdf <- collect_one("apdf_tidy", apdf_tidy(conn = conn, wef = wef, til = til))
    flights <- collect_one("flights_tidy", flights_tidy(conn = conn, wef = wef, til = til))

    if (nrow(apdf) == 0L) {
      warning("apdf_tidy returned 0 rows for ", tag,
              ". Has the month been delivered?", immediate. = TRUE)
    }

    # The same artefacts extract.R writes, from the same queries, so this
    # script is a superset of it rather than a second source of truth.
    write_parquet(apdf, file.path(OUT_DIR, sprintf("apdf_%s.parquet", tag)))
    write_parquet(flights, file.path(OUT_DIR, sprintf("flights_%s.parquet", tag)))

    apdf_all[[tag]] <- apdf
    flights_all[[tag]] <- flights
  }

  apdf <- bind_rows(apdf_all)
  flights <- bind_rows(flights_all)

  # -- restrict to the requested window ------------------------------------
  #
  # On MVT_TIME_UTC, which is the take-off on a DEP row and the landing on an
  # ARR row. A flight departing late on the last day and landing after midnight
  # therefore keeps its departure and loses its arrival. That asymmetry is
  # real and is why the full monthly extracts above are written too: re-window
  # from those rather than re-querying.
  til_exclusive <- to + days(1)
  before <- nrow(apdf)
  apdf <- apdf |> filter(.data$MVT_TIME_UTC >= from, .data$MVT_TIME_UTC < til_exclusive)
  message(sprintf(
    "\nWindowed to %s .. %s: %s of %s movement rows kept.",
    from, to, format(nrow(apdf), big.mark = ","), format(before, big.mark = ",")
  ))

  if (nrow(apdf) == 0L) {
    stop("No movements in the window. Nothing to write.")
  }

  # -- sanity checks, before the pivot hides them --------------------------
  message("\nAPDF SRC_PHASE split (DEP -> AOBT/ATOT, ARR -> ALDT/AIBT):")
  print(count(apdf, SRC_PHASE))

  message("\nPivoting to flight shape ...")
  truth <- pivot_to_flight_shape(apdf)

  # AIRCRAFT_ADDRESS *is* icao24 -- the join key to ADS-B, and the only way
  # this table reaches OPDI's flight list. Without it the truth is unusable,
  # so attach it here rather than leaving every consumer to rediscover it.
  bridge <- flights |>
    filter(!is.na(.data$ID)) |>
    select(ID = "ID", ICAO24 = "AIRCRAFT_ADDRESS", AOBT_3 = "AOBT_3") |>
    distinct(.data$ID, .keep_all = TRUE)
  truth <- left_join(truth, bridge, by = "ID")

  message(sprintf(
    "  %s flights; ICAO24 (the ADS-B join key) missing on %.1f%%",
    format(nrow(truth), big.mark = ","), 100 * mean(is.na(truth$ICAO24))
  ))

  message("\nColumn coverage (what each flight-list column can be checked against):")
  checkable <- c("ATOT", "ALDT", "AOBT", "AIBT", "RWY_DEP", "RWY_ARR",
                 "STND_DEP", "STND_ARR", "C40_DEP", "C40_ARR",
                 "C100_DEP", "C100_ARR")
  for (cc in checkable) {
    message(sprintf("  %-10s %6.1f%% populated", cc,
                    100 * mean(!is.na(truth[[cc]]))))
  }
  message("  C50 / C60 / C110 / C120 -- NO GROUND TRUTH. APDF records 40 NM and")
  message("  100 NM only. Do not report those four columns as validated.")

  tag <- sprintf("%s_%s", format(from, "%Y%m%d"), format(to, "%Y%m%d"))
  out <- file.path(OUT_DIR, sprintf("flight_list_truth_%s.parquet", tag))
  write_parquet(truth, out)
  message("\n", format(nrow(truth), big.mark = ","), " flights -> ", out)

  # -- what to do next -----------------------------------------------------
  message("\nDone. Next:")
  message("  1. Confirm the parquet went through git-lfs, not in as a blob:")
  message("       git add ", OUT_DIR, "/*.parquet")
  message("       git cat-file -p :", out, " | head -3")
  message("     Expect 'version https://git-lfs.github.com/spec/v1' + oid + size.")
  message("  2. Add a row to ", file.path(OUT_DIR, "MANIFEST.md"), ":")
  message(sprintf(
    "       | flight_list_truth_%s.parquet | extract_flight_list_truth.R %s %s | %s | %s |",
    tag, from, to, Sys.Date(), format(nrow(truth), big.mark = ",")
  ))
  message("  3. Commit and push, then pull on the OSN server.")

  invisible(NULL)
}


args <- commandArgs(trailingOnly = TRUE)
main(
  if (length(args) >= 1) args[[1]] else DEFAULT_FROM,
  if (length(args) >= 2) args[[2]] else DEFAULT_TO
)
