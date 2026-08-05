#!/usr/bin/env Rscript
# ---------------------------------------------------------------------------
# Extract EUROCONTROL ground truth (PRISME) to reference/*.parquet
#
# Runs ONLY on the work laptop -- it needs ROracle plus PRU_READ_USR /
# PRU_READ_PWD / PRU_READ_DBNAME. Never run this from the pipeline, from a
# benchmark, or from a Quarto render: `quarto render` must succeed with no
# credentials and no database.
#
#   Rscript reference/extract.R              # defaults to the month below
#   Rscript reference/extract.R 2024-06      # or name one
#
# See reference/README.md for the schema of each extract, and MANIFEST.md for
# what to record afterwards.
# ---------------------------------------------------------------------------

DEFAULT_MONTH <- "2024-06"
OUT_DIR <- "reference"

suppressPackageStartupMessages({
  library(eurocontrol)
  library(arrow)
  library(dplyr)
  library(lubridate)
})


extract_one <- function(label, query, path) {
  message("  ", label, " ...")
  # collect() is required: these are lazy Oracle-backed tables and
  # write_parquet needs them materialised.
  df <- collect(query)
  n <- nrow(df)
  if (n == 0L) {
    warning(
      label, " returned 0 rows. Check the month has been delivered, and see ",
      "the monthly-window note in this script.",
      immediate. = TRUE
    )
  }
  write_parquet(df, path)
  message("    ", format(n, big.mark = ","), " rows -> ", path)
  list(rows = n, data = df)
}


main <- function(month_arg) {
  month <- suppressWarnings(ymd(paste0(month_arg, "-01")))
  if (is.na(month)) {
    stop("Could not parse '", month_arg, "'. Expected YYYY-MM, e.g. 2024-06.")
  }
  if (!dir.exists(OUT_DIR)) {
    stop("Directory '", OUT_DIR, "' not found. Run this from the repo root.")
  }

  # One calendar month at a time. This is a correctness requirement, not a
  # convention: apdf_tidy() filters on MVT_TIME_UTC *and* SRC_DATE_FROM against
  # the same window (airport_operator_data_flow.R:109-114). APDF is delivered
  # monthly, so SRC_DATE_FROM tracks the delivery month -- widen the window and
  # every movement whose source record starts outside it is dropped, with no
  # error and no warning.
  wef <- format(month, "%Y-%m-%d")
  til <- format(month %m+% months(1), "%Y-%m-%d")
  tag <- format(month, "%Y%m")

  message("Extracting ", wef, " -> ", til, " (exclusive)")

  # One connection, reused for both queries, closed however this exits.
  # on.exit only fires inside a function -- hence main().
  conn <- db_connection(schema = "PRU_READ")
  on.exit(
    {
      message("Closing DB connection.")
      try(DBI::dbDisconnect(conn), silent = TRUE)
    },
    add = TRUE
  )

  apdf_path <- file.path(OUT_DIR, sprintf("apdf_%s.parquet", tag))
  flights_path <- file.path(OUT_DIR, sprintf("flights_%s.parquet", tag))

  apdf <- extract_one(
    "apdf_tidy", apdf_tidy(conn = conn, wef = wef, til = til), apdf_path
  )
  flights <- extract_one(
    "flights_tidy", flights_tidy(conn = conn, wef = wef, til = til), flights_path
  )

  # -- sanity checks --------------------------------------------------------
  #
  # APDF is in long/movement form: there is no literal AOBT or ATOT column, and
  # which milestone you get depends on SRC_PHASE. If a phase is missing the
  # benchmark silently covers only departures or only arrivals, so surface the
  # split now rather than discovering it in a paper.
  if (apdf$rows > 0L && "SRC_PHASE" %in% names(apdf$data)) {
    message("\nAPDF SRC_PHASE split (DEP -> AOBT/ATOT, ARR -> ALDT/AIBT):")
    print(count(apdf$data, SRC_PHASE))
  }

  # AIRCRAFT_ADDRESS is the ICAO 24-bit address -- it *is* icao24, and it is
  # the join key to ADS-B. If it is largely NULL the join quietly under-matches.
  if (flights$rows > 0L && "AIRCRAFT_ADDRESS" %in% names(flights$data)) {
    message(sprintf(
      "\nflights: AIRCRAFT_ADDRESS (= icao24, the ADS-B join key) missing on %.1f%% of rows",
      100 * mean(is.na(flights$data$AIRCRAFT_ADDRESS))
    ))
  }

  # -- what to do next ------------------------------------------------------
  message("\nDone. Next:")
  message("  1. Confirm the parquet went through git-lfs, not in as a blob:")
  message("       git add ", apdf_path, " ", flights_path)
  message("       git cat-file -p :", apdf_path, " | head -3")
  message("     Expect 'version https://git-lfs.github.com/spec/v1' + oid + size.")
  message("  2. Add these rows to ", file.path(OUT_DIR, "MANIFEST.md"), ":")
  message(sprintf(
    "       | apdf_%s.parquet | apdf_tidy(wef=\"%s\", til=\"%s\") | %s | %s |",
    tag, wef, til, Sys.Date(), format(apdf$rows, big.mark = ",")
  ))
  message(sprintf(
    "       | flights_%s.parquet | flights_tidy(wef=\"%s\", til=\"%s\") | %s | %s |",
    tag, wef, til, Sys.Date(), format(flights$rows, big.mark = ",")
  ))
  message("  3. Commit and push.")

  invisible(NULL)
}


args <- commandArgs(trailingOnly = TRUE)
main(if (length(args) >= 1) args[[1]] else DEFAULT_MONTH)
