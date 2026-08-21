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

**There is no current measurement of this.** The only figure that exists --
+0.26 pp arrival coverage, -0.12 pp accuracy, departures flat -- comes from the
earlier decimation study, and it should not be quoted as the sampler's benefit:

* one day, 2025-06-05, not the three-day sample;
* measured *before* the ranking rule changed from H3 ring count to exact
  haversine, so on an algorithm since materially altered;
* at `endpoint` 30 NM / 15,000 ft applied to both roles, which is not the
  shipped configuration;
* and it is exactly the kind of carried-over number V6's own rule forbids.

The `decimation` job in this run produces the current figure, both arms, three
days, against the shipped configuration. Whether it clears the 0.5 pp bar is
open until then. The adoption rests on the argument that the bin-based rule is
the correct one and does not depend on the archive carrying positions forward
-- not on a measured gain.

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

## Decision log — autonomous run

Kept as the run proceeds, so every choice made without review is visible.

| # | Decision / fix | Why |
|---|---|---|
| 1 | Stage commands must be scripts, not `python -c` one-liners | The candidate builder was inline and imported `opdi.utils.spark`, which does not exist. It died an hour into a run on a typo nothing could check. Now `benchmarks/build_candidates.py`, and every stage script is verified to import before launching. |
| 2 | Dropped stages `01_ingest_statevectors` and `02_tracks` | Both claimed tables `01_02_rebuild_sample` already produces, and `02_tracks` claimed the *same* manifest key, so their provenance entries silently overwrote each other. |
| 3 | Resumed without `--force` after the failure | The sample rebuild and reference data were already complete and correct. Re-running them would have cost another hour and risked the same fragile overwrite window for no gain; staleness detection was left to decide the rest, and it correctly rebuilt candidates and the 2025 vote cache while skipping the 2024 one. |
| 4 | Did **not** rebuild the 2024 endpoint candidate cache | Its input (`research/tracks`, the 2024 period) did not change, so it is still valid. Confirmed by the chain skipping it as current. |
| 5 | Corrected the sampler claim rather than leaving it | The +0.26 pp figure was from the earlier one-day study, taken before the ranking change and at an operating point that is not the shipped configuration. Quoting it as the sampler's benefit broke this study's own rule against carried-over numbers. |

| 6 | `trend_bearing` scored 0.00% on every variant — fixed and re-run | ADS-B callsigns are space-padded to eight characters and the ground-truth callsign is not, so the identity join matched nothing. The job wrote a CSV of zeros and exited successfully. Now trims, and treats zero coverage on both roles as an error rather than a result. |

## Observations worth checking

* **State vectors grew 58% on disk** (6.13 -> 9.68 GB) while row count should be
  within 0.2%. Tracks came out the same size as before (10.16 GB), which points
  at file layout and compression rather than rows. Worth confirming against the
  row count if it matters, but nothing depends on it.
* **Bucket usage** reached ~84 GB mid-run against a ~100 GB quota. The tracks
  overwrite frees its old copy before writing, so the peak stayed inside the
  quota, but there is not much room.

## Interim result: what the sampler is actually worth

First current measurement, three days, 371 trend sweep cells, bucket against
modulo with everything else identical (the previous CSV is in git history and
was produced by the same harness on the modulo sample).

| | median dscore | cells improved | cells worsened | median dcoverage |
|---|---|---|---|---|
| ADEP | +6 | 231 | 134 | +0.002 pp |
| ADES | +17 | 310 | 57 | +0.004 pp |

**Consistent in direction, negligible in magnitude.** 310 of 371 arrival cells
improving is not noise -- the bin-based rule really does help -- but the size is
about +0.004 pp of coverage, two orders of magnitude below the 0.5 pp bar and
far below the +0.26 pp the earlier one-day study suggested.

At the legacy cell specifically the effect is nil: ADEP -23 score, ADES -1.

This is the trend harness. The endpoint sweeps and the `decimation` job give the
endpoint-side view, which is where the earlier study measured its +0.26 pp, and
those are still to come.

**What this does not change:** the argument for adopting the bin-based sampler
was never the size of the gain. It is that the rule is correct and does not
depend on the OpenSky archive carrying positions forward. The measurement says
the cost of that correctness is nil and the benefit is small and real, which is
a perfectly good reason to keep it -- it just should not be sold as a coverage
improvement.

## The sampler, settled

Both sides now measured on three days, same code, full grids, bucket against
modulo.

| | median dscore | improved | worsened | median dcoverage |
|---|---|---|---|---|
| trend sweep, ADEP (371 cells) | +6 | 231 | 134 | +0.002 pp |
| trend sweep, ADES (371 cells) | +17 | 310 | 57 | +0.004 pp |
| endpoint grid, ADEP (126 cells) | -6 | 41 | 84 | +0.001 pp |
| endpoint grid, ADES (126 cells) | -12 | 28 | 92 | -0.014 pp |

At the shipped operating point: `endpoint` 30 NM / 15,000 ft gives ADEP -18 and
ADES -4; the trend side gives ADES +17. The shipped configuration takes
departures from `endpoint` and arrivals from `trend`, so the net is **about
zero**.

**The earlier +0.26 pp does not reproduce.** Measured on three days on the same
rule it was measured on, the endpoint arrival coverage change is -0.014 pp. The
one-day figure was noise, and it was the number used to characterise the
sampler as beneficial-but-below-bar. Neither half of that was right: it is not
below bar, it is *not measurably different from zero*.

**Caveat.** Rebuilding the sample changed `track_id` values, because the
rescued rows sit at track boundaries. So the benchmark's track-to-flight
alignment differs slightly between the two arms, and some of these small deltas
are that rather than the sampler. That cuts both ways and is another reason not
to read anything into a +-0.02 pp difference.

**What this means for the decision.** The case for the bin-based sampler was
never a coverage gain -- it is that the rule is correct and does not depend on
the OpenSky archive carrying positions forward. The measurement now says the
correctness costs nothing, which is the useful thing to know. It should be
documented as neutral on accuracy and adopted on principle, not sold as an
improvement.

## New result: bearing improves `trend`, as a tie-break

The comparison that had never been made. `trend` is what ships for arrivals and
it fails by *misnaming*, so the variants that suit it are a rerank or a
tie-break -- not the rescue used on the endpoint family.

| variant | ADES cov | ADES acc | correct | wrong | vs shipped |
|---|---|---|---|---|---|
| base: trend as shipped | 69.76% | 97.33% | 64,583 | 1,769 | — |
| tie-break within 1 NM | 69.76% | 97.75% | 64,857 | 1,495 | +822 |
| **tie-break within 2 NM** | 69.76% | **97.91%** | 64,963 | **1,389** | **+1,140** |
| tie-break within 5 NM | 69.76% | 95.77% | 63,542 | 2,810 | -3,123 |
| rerank by alignment | 69.76% | 69.07% | 45,830 | 20,522 | -56,259 |
| veto (best, >10 deg) | 20.39% | 99.35% | 19,263 | 127 | -42,036 |

**+0.58 pp accuracy at zero coverage cost** -- 380 wrong answers become correct.
Interior optimum at 2 NM. This clears the 0.5 pp bar, and unlike a coverage
change it involves no trade-off at all: the same flights are answered, more of
them correctly.

Why the shape makes sense: alignment cannot *name* an aerodrome, because every
aerodrome on the same radial behind the right one is equally aligned -- hence
rerank losing 56,259. But it can separate two candidates that are already
nearly equidistant, which is exactly what the distance band restricts it to.

**Not adopted.** This is a new algorithmic component, not a parameter: the
pipeline would need the track course computed in the flight-list path. One
period, one measurement. It is the strongest candidate for the next change and
should be reviewed rather than shipped overnight.

## v6.1 feasibility census (2026-08-21)

Run before any code changed, to find out whether the sample can answer the
question the study asks. `benchmarks/elevation_census.py`, both periods,
in-area ground truth only.

Ground-truthed movements per field-elevation band:

| Band | 2024-06 arr / dep | 2025-06 arr / dep | Aerodromes (2025 arr / dep) |
|---|---|---|---|
| `<500` | 65,176 / 65,156 | 66,637 / 66,648 | 490 / 497 |
| `500-1500` | 15,154 / 15,223 | 15,287 / 15,354 | 164 / 169 |
| `1500-3000` | 4,239 / 4,218 | 4,434 / 4,430 | 77 / 78 |
| `>3000` | 686 / 691 | 706 / 705 | 26 / 26 |
| `unknown` | 163 / 159 | 117 / 115 | 48 / 54 |

**The gate passes.** The bar was ~200 movements in `>3000` per period across at
least 5 aerodromes; the sample carries ~700 per role across 26. The two periods
agree closely, which is what makes the second one worth having.

**Two things the census changed about the study.**

**1. The treatment bands are dominated by one aerodrome each.** Madrid (LEMD,
1,998 ft) alone carries 3,559 of the ~8,900 movements in `1500-3000`, and
Ankara (LTAC, 3,125 ft) carries 756 of the ~1,411 in `>3000` -- 40% and 54%
respectively. A band-level gain is therefore not automatically an elevation
effect; it could be a Madrid effect or an Ankara effect wearing a band's name.

Arm C must report a **per-aerodrome view and a leave-one-out check** on each
treatment band, not band means alone. Without it the study's discriminating
measurement is not actually discriminating, which would leave the whole design
resting on a number that cannot fail.

**2. The expected effect is smaller than the design's motivating example.** The
spec reasons from Erzurum at 5,763 ft, where FL60 leaves about 240 ft to vote
in. Erzurum is real but rare: it does not reach the top fifteen elevated
aerodromes by traffic, so it carries fewer than ~110 movements. The elevated
traffic that actually exists sits at 1,500-3,500 ft -- LEMD keeps ~4,100 ft of
vote room under FL60, LTAC ~2,975 ft. That is a reduction in vote room, not its
near-elimination.

The paper should be written from these numbers rather than from the Erzurum
case, which illustrates the mechanism honestly but overstates how much of the
sample it applies to.

Busiest aerodromes at or above 1,500 ft, 2025 sample:

| Aerodrome | Elevation ft | Movements |
|---|---|---|
| LEMD Madrid | 1,998 | 3,559 |
| LTAC Ankara | 3,125 | 756 |
| GCXO Tenerife North | 2,076 | 639 |
| GMMX Marrakesh | 1,545 | 557 |
| LBSF Sofia | 1,742 | 540 |
| UGTB Tbilisi | 1,624 | 514 |
| UDYZ Yerevan | 2,838 | 384 |
| BKPR Pristina | 1,789 | 240 |
| OJAI Amman | 2,395 | 232 |
| EDJA Memmingen | 2,077 | 206 |
