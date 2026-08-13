#!/usr/bin/env Rscript
# ---------------------------------------------------------------------------
# Extract APDF *including* the column families apdf_tidy() drops.
#
# Runs ONLY on the work laptop -- it needs ROracle plus PRU_READ_USR /
# PRU_READ_PWD / PRU_READ_DBNAME. Never run this from the pipeline, from a
# benchmark, or from a Quarto render.
#
#   Rscript reference/extract_apdf_full.R 2025-06
#   Rscript reference/extract_apdf_full.R 2024-06
#
# WHY THIS EXISTS
#
# `apdf_tidy()` selects starts_with("C40_") / starts_with("C100_") and then
# immediately drops four families again
# (airport_operator_data_flow.R:133-139):
#
#     -ends_with("_MIN"), -ends_with("_IN_FRONT"),
#     -ends_with("_CTFM"), -ends_with("_CPF"), -contains("TRANSIT")
#
# For benchmarking an ADS-B-derived ring crossing, three of those are more
# valuable than what survives:
#
#   *_CPF   correlated position flight -- crossings derived from raw radar
#           plots. The closest thing in PRISME to what ADS-B itself sees, and
#           therefore the fairest comparator. The default C40_CROSS_TIME is
#           airport-reported and of unstated provenance.
#   *_CTFM  the Current Tactical Flight Model (M3) trajectory. Lets a
#           disagreement be attributed -- "OPDI is wrong" versus "the two
#           EUROCONTROL sources disagree with each other".
#   *TRANSIT*
#           ASMA transit times, which are direct ground truth for ICAO KPI08
#           (additional time in terminal airspace) rather than something we
#           have to reconstruct from a crossing time and a landing time.
#
# This writes a SEPARATE file rather than replacing apdf_<tag>.parquet, so the
# existing committed extracts stay byte-for-byte reproducible and nothing that
# already cites them changes underneath.
#
# The `eurocontrol` package is reference material and is NOT edited: this uses
# the exported low-level accessor `apdf_tbl()` and applies its own select.
# ---------------------------------------------------------------------------

OUT_DIR <- "reference"

suppressPackageStartupMessages({
  library(eurocontrol)
  library(arrow)
  library(dplyr)
  library(lubridate)
})


#' APDF for one month with nothing dropped from the ring families.
#'
#' The filter is copied verbatim from `apdf_tidy()` and must stay that way.
#' It constrains SRC_DATE_FROM as well as MVT_TIME_UTC: APDF is delivered
#' monthly and SRC_DATE_FROM tracks the delivery month, so a window wider than
#' one calendar month silently drops every movement whose source record starts
#' outside it -- no error, no warning, just fewer rows.
apdf_full <- function(conn, wef, til) {
  wef <- lubridate::as_datetime(wef, tz = "UTC") |> format("%Y-%m-%d %H:%M:%S")
  til <- lubridate::as_datetime(til, tz = "UTC") |> format("%Y-%m-%d %H:%M:%S")

  apdf_tbl(conn) |>
    dplyr::filter(
      TO_DATE(wef, "yyyy-mm-dd hh24:mi:ss") <= .data$MVT_TIME_UTC,
      .data$MVT_TIME_UTC < TO_DATE(til, "yyyy-mm-dd hh24:mi:ss"),
      TO_DATE(wef, "yyyy-mm-dd hh24:mi:ss") <= .data$SRC_DATE_FROM,
      .data$SRC_DATE_FROM < TO_DATE(til, "yyyy-mm-dd hh24:mi:ss")
    ) |>
    dplyr::select(
      "APDS_ID",
      "ID" = "IM_SAMAD_ID",
      "AP_C_FLTID",
      "AP_C_FLTRUL",
      "AP_C_REG",
      dplyr::ends_with("ICAO"),
      "SRC_PHASE",
      # Carried so the monthly-window caveat can be audited from the extract
      # itself rather than trusted. apdf_tidy() filters on it but never
      # returns it.
      "SRC_DATE_FROM",
      "MVT_TIME_UTC",
      "BLOCK_TIME_UTC",
      "SCHED_TIME_UTC",
      "ARCTYP",
      "AP_C_RWY",
      "AP_C_STND",
      # Everything, including _CPF / _CTFM / _MIN / _IN_FRONT / TRANSIT.
      dplyr::starts_with("C40_"),
      dplyr::starts_with("C100_")
    )
}


main <- function(month_arg) {
  month <- suppressWarnings(ymd(paste0(month_arg, "-01")))
  if (is.na(month)) {
    stop("Could not parse '", month_arg, "'. Expected YYYY-MM, e.g. 2025-06.")
  }
  if (!dir.exists(OUT_DIR)) {
    stop("Directory '", OUT_DIR, "' not found. Run this from the repo root.")
  }

  wef <- format(month, "%Y-%m-%d")
  til <- format(month %m+% months(1), "%Y-%m-%d")
  tag <- format(month, "%Y%m")
  path <- file.path(OUT_DIR, sprintf("apdf_full_%s.parquet", tag))

  message("Extracting ", wef, " -> ", til, " (exclusive)")

  conn <- db_connection(schema = "PRU_READ")
  on.exit(
    {
      message("Closing DB connection.")
      try(DBI::dbDisconnect(conn), silent = TRUE)
    },
    add = TRUE
  )

  df <- collect(apdf_full(conn, wef, til))
  n <- nrow(df)
  if (n == 0L) {
    warning(
      "0 rows. Check the month has been delivered, and see the ",
      "monthly-window note above.",
      immediate. = TRUE
    )
  }
  write_parquet(df, path)
  message("  ", format(n, big.mark = ","), " rows -> ", path)

  # -- what actually came back ---------------------------------------------
  #
  # The point of the exercise is the columns, so print them. The upstream
  # names of the recovered families are not documented anywhere in this
  # workspace, and the benchmark has to address them by name.
  recovered <- grep("_CPF$|_CTFM$|_MIN$|_IN_FRONT$|TRANSIT", names(df), value = TRUE)
  message("\nColumns: ", length(names(df)), " total, ",
          length(recovered), " recovered by this script.")
  message("Recovered:")
  for (nm in recovered) {
    filled <- 100 * mean(!is.na(df[[nm]]))
    message(sprintf("  %-32s %6.2f%% filled", nm, filled))
  }

  # A recovered column that is empty is worse than useless -- it looks like
  # ground truth and answers nothing. Say so now, not in a paper.
  empty <- recovered[vapply(recovered, function(nm) all(is.na(df[[nm]])), logical(1))]
  if (length(empty) > 0L) {
    warning("Entirely NULL, do not benchmark against these: ",
            paste(empty, collapse = ", "), immediate. = TRUE)
  }

  if ("SRC_PHASE" %in% names(df)) {
    message("\nSRC_PHASE split:")
    print(count(df, SRC_PHASE))
  }

  message("\nDone. Next:")
  message("  1. Confirm it went through git-lfs, not in as a blob:")
  message("       git add ", path)
  message("       git cat-file -p :", path, " | head -3")
  message("     Expect 'version https://git-lfs.github.com/spec/v1' + oid + size.")
  message("  2. Add a row to ", file.path(OUT_DIR, "MANIFEST.md"), ":")
  message(sprintf(
    "       | apdf_full_%s.parquet | extract_apdf_full.R (%s -> %s) | %s | %s |",
    tag, wef, til, Sys.Date(), format(n, big.mark = ",")
  ))
  message("  3. Commit, then mirror to S3 so the executors can read it:")
  message("       aws s3 cp ", path,
          " s3://eurocontrol/opdi/research/reference/ --endpoint-url https://s3.opensky-network.org")

  invisible(NULL)
}


args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1) {
  stop("Give a month: Rscript reference/extract_apdf_full.R 2025-06")
}
main(args[[1]])
