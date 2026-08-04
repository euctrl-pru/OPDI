# `reference/` — EUROCONTROL ground-truth extracts

Committed parquet extracted from the PRISME Oracle warehouse via the
[`eurocontrol`](https://github.com/eurocontrol/eurocontrol) R package. These files are the
**benchmark truth** that OPDI milestones are validated against, and the **only** source of
EUROCONTROL data that the pipeline, the benchmarks, or a Quarto paper may read.

## Rules

1. **Never query PRISME from the pipeline, a benchmark, or a paper render.** Extraction is a
   separate, manual, one-directional step. `quarto render` must succeed with no credentials and
   no database — that is an existing portal guarantee.
2. **Extraction runs only on the work laptop.** It needs ROracle plus `<SCHEMA>_USR`,
   `<SCHEMA>_PWD` and `<SCHEMA>_DBNAME` environment variables (`eurocontrol/R/db.R:32-34`).
   No other machine — including OSN — has warehouse access.
3. **Everything here goes through git-lfs.** See the warning below.

```
eurocontrol (R, work laptop)  →  arrow::write_parquet()  →  opdi/reference/  →  git-lfs  →  OSN
```

OSN pulls these with a shallow clone of *this* repo — never the meta-repo:

```bash
git clone --depth 1 https://github.com/euctrl-pru/opdi
cd opdi && git lfs pull --include='reference/**'
```

This is why `reference/` lives in `opdi/` and not at the workspace root: the meta-repo is never
cloned on OSN, so anything stored there would be invisible to the pipeline.

## ⚠️ git-lfs must be installed *before* you add any parquet

`.gitattributes` in the repo root declares `reference/**/*.parquet` as lfs-tracked, but that
declaration is inert unless git-lfs is actually installed and initialised. If you `git add` a
parquet without it, the file is committed as a **normal blob**, permanently bloating history,
and the `.gitattributes` rule will silently not apply.

```bash
git lfs version        # must print a version — if it errors, stop
git lfs install --local
git lfs track          # should list reference/**/*.parquet
```

After committing, verify the file really went to lfs:

```bash
git lfs ls-files                       # the parquet must appear here
git cat-file -p HEAD:reference/<f>.parquet | head -3   # must show an lfs pointer, not binary
```

An lfs pointer looks like `version https://git-lfs.github.com/spec/v1` + `oid sha256:…` + `size …`.
If you see raw parquet bytes instead, the file bypassed lfs — undo the commit and redo it.

## Naming convention

```
reference/
├── README.md
├── MANIFEST.md                    # one row per extract: file, R call, date, row count
├── apdf_<YYYYMM>.parquet          # APDF movements, one file per calendar month
└── flights_<YYYYMM>.parquet       # flight-level truth, one file per calendar month
```

One file per month, always. Never concatenate months into a single file — see the loop caveat.

## The extracts

### `apdf_<YYYYMM>.parquet` — airport movement milestones

From `eurocontrol::apdf_tidy(wef, til)`. **APDF is in long / movement form: there is no literal
AOBT or ATOT column.** Each row is one movement, and the milestone you get depends on
`SRC_PHASE`:

| OPDI milestone | Column | Filter |
|---|---|---|
| AOBT | `BLOCK_TIME_UTC` | `SRC_PHASE == 'DEP'` |
| **ATOT** | `MVT_TIME_UTC` | `SRC_PHASE == 'DEP'` |
| **ALDT** | `MVT_TIME_UTC` | `SRC_PHASE == 'ARR'` |
| AIBT | `BLOCK_TIME_UTC` | `SRC_PHASE == 'ARR'` |

Other benchmark columns: `AP_C_RWY` (runway ID), `AP_C_STND` (stand ID), `AP_C_FLTID`
(callsign), and the ASMA ring crossings `C40_CROSS_{TIME,LAT,LON,FL}` /
`C100_CROSS_{TIME,LAT,LON,FL}` plus `_BEARING`. For a departure these are the *first* crossing;
for an arrival, the *last*.

> **⚠️ Loop monthly — a wide window silently drops rows.**
> `apdf_tidy` filters on **two** date fields against the same window
> (`eurocontrol/R/airport_operator_data_flow.R:109-114`): `MVT_TIME_UTC` **and**
> `SRC_DATE_FROM`, the validity start of the source record. APDF is delivered monthly, so
> `SRC_DATE_FROM` tracks the delivery month. Ask for a whole year in one call and every movement
> whose source record starts outside that window is dropped — with no error and no warning.
> Always iterate one calendar month at a time.

### `flights_<YYYYMM>.parquet` — flight-level truth

From `eurocontrol::flights_tidy(wef, til)`. Supplies `ADEP` / `ADES` (the ADEP/ADES coverage
benchmark), `AOBT_3`, `FLT_TOW`, and critically **`AIRCRAFT_ADDRESS` — the ICAO 24-bit address,
which *is* `icao24`** in the ADS-B data. That is the join key.

Join to OPDI output on `AIRCRAFT_ADDRESS` (= `icao24`) + callsign (`AIRCRAFT_ID` /
`AP_C_FLTID`) + date. `flights_tidy` defaults to `icao_flt_types = c('S', 'N')` (scheduled and
non-scheduled) and excludes sensitive, military and HEAD flights unless asked otherwise — keep
the defaults so the population is reproducible, and record any deviation in `MANIFEST.md`.

## Extraction recipe

Run on the work laptop, one month per iteration:

```r
library(eurocontrol)
library(arrow)
library(lubridate)

months <- seq(ymd("2024-01-01"), ymd("2024-12-01"), by = "month")

for (m in months) {
  m   <- as_date(m)
  wef <- format(m, "%Y-%m-%d")
  til <- format(m %m+% months(1), "%Y-%m-%d")
  tag <- format(m, "%Y%m")

  apdf_tidy(wef = wef, til = til) |>
    dplyr::collect() |>
    write_parquet(sprintf("reference/apdf_%s.parquet", tag))

  flights_tidy(wef = wef, til = til) |>
    dplyr::collect() |>
    write_parquet(sprintf("reference/flights_%s.parquet", tag))
}
```

`collect()` is required — these are lazy Oracle-backed tables, and `write_parquet` needs them
materialised.

## Recording an extract

Every file added here gets a row in `MANIFEST.md`: filename, the exact R call including all
non-default arguments, the extraction date, and the row count. The extraction date matters —
APDF is restated over time, so two extracts of the same month taken months apart will differ,
and a benchmark result is only reproducible against the extract it was computed from.
