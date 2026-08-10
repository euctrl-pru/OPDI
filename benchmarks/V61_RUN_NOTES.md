# v6.1 run notes

Written as the run proceeds, so the state is legible without reading a log.

## What is running

`python benchmarks/regenerate_v6.py --with-stages --force --rebuild-stage 01_02_rebuild_sample`

Full end-to-end: state-vector ingest with the **bucket** sampler, tracks,
reference zones, endpoint candidates and both vote caches, then all twelve
analysis jobs, then the report renders from what they produce.

Log: `$CLAUDE_JOB_DIR/tmp/v61c/chain.log`

## The change under test

`ingestion.decimation` now defaults to `"bucket"` — keep the newest row in each
5 s window — instead of `"modulo"`, which keeps a row only if one exists at the
single second per window congruent to zero.

Measured previously on one day: **+0.26 pp arrival coverage, −0.12 pp accuracy**,
departures flat. Below the 0.5 pp bar, adopted anyway on the argument that the
bin-based rule is the correct one and does not depend on the OpenSky archive
carrying positions forward. v6.1 re-measures it on three days.

**`track_id` values will differ** from those published under the modulo rule,
because the rescued rows sit at track boundaries and the splitter breaks on
gaps. Accepted deliberately. It needs a new algorithm `version`, not a mutation
of an existing one — **this is still outstanding and must be done before any
republication.**

## Bugs found and fixed during launch

All three were silent wrong-size failures rather than crashes, which is the
failure mode this study keeps producing.

1. **Append where overwrite was needed.** Steps 01 and 02 call `write_table`
   with no mode, which defaults to append. Clearing the progress logs and
   re-running — the obvious way to rebuild — would have doubled both tables:
   +16.5 GB against 19.8 GB of headroom, and every downstream aggregate
   double-weighted. A doubled table does not fail. Fixed by forcing the first
   write of a rebuild to overwrite.

2. **Exclusive end date.** `ingest_from_s3` treats `end_date` as exclusive, so
   `--end 2025-06-07` ingested two days, not three — 48 hourly partitions
   instead of 72. Caught from the log line. Production data was not damaged:
   Spark commits an overwrite at the end and the process was killed mid-write,
   verified afterwards at 469 objects / 6.13 GB.

3. **A fix that did not apply.** The edit for (2) failed an assertion inside a
   heredoc whose output was buried, and the chain was relaunched anyway,
   repeating (2). Now verified in the file before launching.

## Earlier in the session, for context

* Candidate ranking changed from **H3 ring count** to **exact haversine**. This
  is the substantive result: every tuned `trend` parameter fails under ring
  selection and gains under exact distance, because ring selection discards
  candidates before measuring them. `DetectionConfig.legacy()` pins `"ring"`.
* Shipped defaults are now FL60 / 20 NM / margin 2 / penalty 10 for `trend`,
  and 30 NM / 15,000 ft / penalty 10 for `endpoint`.
* The scheduled-service penalty was inert in the pipeline — the projection
  dropped `apt_scheduled` before the ranking looked for it.

## Still outstanding after this run

* A new algorithm `version` string for the sampler and ranking changes.
* Whether `trend` + bearing beats the shipped configuration (first measurement
  lands in this run).
* Whether the 2024 endpoint grid agrees with 2025 on 30 NM / 15,000 ft.
* The report sections that read the new CSVs state their own conclusions from
  the data, including sign, so they do not presume the answer.
