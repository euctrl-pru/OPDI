# Measuring a correction with the scorecard

This guide is for the people making the corrections decided after the spot
checks of 2026-06-14 (`DECISIONS.md` of the spot checks, six work packages).
Every correction is judged on the same real day with one tool,
`benchmarks/corrections_scorecard.py`. It runs the events step on a
verification sample of that day (or on the whole day), scores the result, and
compares it with the baseline of today's code.

## The rule for Spark on this machine: one lock

The pod has 16 GB of memory for everything that runs on it, and a process
that goes over the limit is killed without a message. Six people work at the
same time in separate worktrees. So **every command that starts Spark waits
for one lock**, `/home/jupyter/.claude/jobs/616d5918/tmp/spark.lock`, and only
one runs at a time.

- `run`, `score`, `compare` and `subset` take the lock themselves and
  release it when they finish. While they wait they print `waiting for the
  Spark lock ...`. `agree`, `render`, `clean` and `compare --no-content`
  start no Spark.
- Everything else that starts Spark must be wrapped by hand, with a wait of
  up to 6 hours:

  ```bash
  flock -w 21600 /home/jupyter/.claude/jobs/616d5918/tmp/spark.lock <command>
  ```

  This includes **pytest**, as soon as a test uses the `spark` fixture
  (most of `tests/`):

  ```bash
  flock -w 21600 /home/jupyter/.claude/jobs/616d5918/tmp/spark.lock \
      .venv310/bin/python -m pytest tests/test_runway_ops.py -q
  ```

- A scorecard command started under `flock` notices that its parent holds the
  lock and goes ahead (it does not wait for itself).
- The lock is shared with long runs: a full-day run holds it for about half
  an hour. Start a long command detached (below) instead of waiting at the
  terminal.

## Before the first run in a new worktree

A new git worktree has no S3 credentials and no Python environment. Link
those of the main checkout:

```bash
cd <your worktree>
ln -s /home/jupyter/work/opdi-workspace/opdi/.env .env
ln -s /home/jupyter/work/opdi-workspace/opdi/.venv310 .venv310
export PYSPARK_DRIVER_PYTHON=$PWD/.venv310/bin/python
unset PYSPARK_PYTHON
```

Do not `source .env`: the scripts read it themselves.

## The three commands

All three run from your worktree. `run` uses the pipeline code of the
worktree the script is in (or of `--code <worktree>`), so a branch cut from
`industrialisation` measures its own changes. Labels are short names of your
choice (`wp2_heading`, `wp2_heading_off`); they become folders.

```bash
# 1. Run the events step (04 and 04b) on the sample with your code.
.venv310/bin/python -u benchmarks/corrections_scorecard.py run --scope sample --label wp2_heading
# 2. Score it.
.venv310/bin/python -u benchmarks/corrections_scorecard.py score wp2_heading
# 3. Compare with the baseline of today's code.
.venv310/bin/python -u benchmarks/corrections_scorecard.py compare base wp2_heading
```

- Work package 4 changes the flight list: add `--steps 03 04 04b` so step 03
  runs too, from the cleaned tracks.
- `--scope day` runs the whole day instead of the sample; its baseline is
  `base_day` (`compare base_day <yours>`). Run it once before you finish, to
  confirm what the sample showed, and for the counters that only make sense
  on the whole day (below).
- `--code <worktree>` runs the pipeline of another worktree with this
  worktree's scorecard, for a branch that does not hold the scorecard yet.
- `--replace` runs a label again; without it an existing label is refused.
- `clean <label>` deletes a run's tables on the bucket. The baselines
  `base` and `base_day` cannot be deleted this way.

Long commands are best started detached, with their own log, and waited for
on the log's last line:

```bash
setsid nohup bash -c '.venv310/bin/python -u benchmarks/corrections_scorecard.py run --scope sample \
    --label wp2_heading > run_wp2.log 2>&1; echo "run exit $?" >> run_wp2.log' > /dev/null 2>&1 &
until grep -q "run exit" run_wp2.log; do sleep 30; done; tail -3 run_wp2.log
```

## Turning a correction off and on

Every correction has its own configuration field (on by default, off in the
v0.0.2 presets). `run --set group.field=value` changes one field for one run,
and can be repeated. `group` is the attribute of `OPDIConfig`: `events`,
`detection`, `cleaning`, `segmentation`. For example:

```bash
.venv310/bin/python -u benchmarks/corrections_scorecard.py run --scope sample \
    --label wp2_off --set events.runway_take_off_needs_air_report=false
.venv310/bin/python -u benchmarks/corrections_scorecard.py score wp2_off
.venv310/bin/python -u benchmarks/corrections_scorecard.py compare wp2_off wp2_heading
```

A field that does not exist stops the run before Spark starts. The settings
of a run are kept in its `run.json` and printed at the top of its scorecard.

## How long it takes

Measured on the baseline (13 executors, one command at a time):

| command | sample | whole day |
|---|---:|---:|
| `run` (steps 04 and 04b) | about 15 min (step 04: 806 to 831 s) | 30 to 35 min (step 04: 1,507 to 1,787 s) |
| `run --steps 03 04 04b` | add about 3 min | add about 6 min (step 03: 366 s) |
| `score` | about 10 min | about 18 min |
| `compare` | about 4 min | about 10 min |
| `agree` | seconds, no Spark | |

Most of the sample's step 04 is a fixed cost: two stages of the events step
that join its 18 families take about 220 s each whatever the size of the
data, so a sample of a third of the reports takes half the time of the day.
The same sample run differed by 2% to 3% in time (806, 821 and 831 s), and
three full-day runs of the events step took 1,507, 1,549 and 1,787 s. A
slowdown near the 5% limit is therefore measured again (both runs, twice)
before it is believed; on the full day one run says little.

Two runs with the same code and settings give identical tables (checked on
the sample: all 49 event types, every event and every field, the two
measurements and all 44 flight-list columns were equal).

## Where things are

- The sample: `s3a://eurocontrol/opdi-world/research/spot/dsub/` (cleaned
  tracks, the flight list of step 03, the endpoint candidates).
  It holds 39,185 of the day's 163,108 tracks: 33,818 matched to an
  airport record or a Network Manager flight, 1,596 more from the readers'
  examples, and 3,771 drawn at random (3% of the rest, always the same).
  That is 55.3 of 157.8 million reports (35%), 3.6 of 9.2 GB, and 39,144
  flight-list rows. The tracks are long airline flights mostly, so the
  sample holds a third of the reports for a quarter of the tracks.
- The whole day: cleaned tracks in `research/spot/d0614/`, the flight list
  and candidates of step 03 in `research/spot/d0614_03/`.
- Each run: `research/spot/runs/<label>/` on the bucket (events,
  measurements, removed events, flight list); locally in
  `/home/jupyter/.claude/jobs/616d5918/tmp/score/runs/<label>/`:
  `run.json` (code, settings, time per step), `scorecard_<label>.csv` and
  `.md`, `confirmed_<label>.parquet` (the events the airport records
  confirm), `events_<label>.parquet`.
- Comparisons: `/home/jupyter/.claude/jobs/616d5918/tmp/score/compare/<before>_vs_<after>/`.
- The bucket has a 200 GB limit. A sample run writes about 0.45 GB, a
  full-day run about 1.5 GB; delete your runs with `clean` when they
  are no longer needed.

## Reading the scorecard

The scorecard has five sections. Each line is `section, item, measure,
value, count, finding`; `count` is the number of cases the value rests on.

- **i. Against airport records.** For each item (AOBT, AIBT, ATOT and ALDT
  by method, take-off-roll, line-up, runway-vacated, the threshold crossing,
  the 40 and 100 NM rings, the stand passages): the events, the records of
  the day for the scope's flights, the events with a record, the share of
  records found, the share within 30, 60 and 300 s, the median and 90th
  percentile of the difference, and the mean difference (event minus record,
  within 300 s). A record is matched on the aircraft address, the call sign
  **and the airport**, the nearest within 6 h; without the airport, another
  leg of the same aircraft is matched. Many airports give times to the whole
  minute, so the line "share within 60 s (records with seconds)" is the
  sharper one. Then the runway named against the record's runway (exact,
  reciprocal, other, none named, no record runway), from the events and from
  the flight list, and the stand against the record's stand.
- **ii. Against Network Manager.** Departure and destination airport right,
  wrong or none, by source; the aircraft type; the distance flown at the
  last event against the route length.
- **iii. Odd cases.** One counter per cause the readers found. `value` is
  the number of odd cases and `count` the number of items they are counted
  among, so the share is value / count. `finding` names the reader's file
  and cause (`a2.C7` is cause C7 of `a2.md`) and the work package
  (`WP2.3`). The counter's exact definition is next to its code in
  `event_counters()`, `count_other()` and `count_flight_list()`. Lines named
  `info.<field> = <value>` count the new `info` fields of the corrections
  wherever they appear.
- **iv. Counts and fill rates.** Events per type, flights, and the share of
  flight-list rows with each column filled.
- **v. Time.** Seconds and executor-seconds of each step.

`compare <before> <after>` writes, in the comparison folder:

- `compare.md`: the verdict, the scorecard lines that changed (unchanged
  lines are left out), and the confirmed events lost or moved, with the
  track, type, both times, the record and `info`;
- `scorecards.csv`: every line of both scorecards with the change;
- `confirmed_lost_or_moved.csv`: every event within 60 s of its airport
  record in `<before>` that `<after>` does not have at the same time
  (within 1 s): `lost` when no event of that type of the flight is within
  60 s of the record any more, `moved` when one still is;
- `content.csv`: every event type and the flight list compared on content,
  as `benchmarks/day_compare.py` does. A type a correction does not concern
  should show every event matched and no field differing.

The exit code is 1 when a confirmed event is lost (`--allow N` accepts N
agreed exceptions, each read and explained) or when the events step is more
than 5% slower than in `<before>`. Compare a run with the baseline of the
same scope only.

## Sample and full day

The sample holds every track of the day that an airport record or Network
Manager knows, so sections i and ii give the same figures on the sample as
on the whole day. Section iii does not always: the sample is mostly airline
flights in and over Europe, and the day also holds light aircraft, vehicles,
local flights and the rest of the world. On the baseline, all 126 lines of
section i and all 29 shares of section ii agree between sample and day
(within 2 points or a fifth of the day's value); of section iii, 759 lines
agree, 92 differ and 28 rest on fewer than 30 cases on the sample.

Use the sample to see whether a correction moves its counter in the right
direction and what it does to the confirmed events. Use the full day for
the size of an odd case, and always for these counters, which only make
sense there:

- departure equal to destination (all its lines), surface and block events
  of tracks that never leave the ground, stand names "None", tops with no
  climb or descent before or after them, holdings not followed by a landing,
  several merge points on one track: their share among all flights differs
  from their share among airline flights;
- the superseded flags: they compare the rows of one aircraft, and the
  sample does not hold every row of an aircraft;
- rows with no day (DOF NULL) are written by step 03: a correction of them
  shows only in a run with `--steps 03 04 04b` (the other runs start from
  the rows step 03 wrote for the baseline);
- tracks with events but no flight-list row: only the random part of the
  sample holds such tracks.

The line-by-line agreement of the baseline is in
`/home/jupyter/.claude/jobs/616d5918/tmp/score/compare/base_vs_base_day_agreement.md`
(`agree base base_day` writes it again).
