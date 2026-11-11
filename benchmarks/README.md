# Study scripts

This folder holds the scripts behind the OPDI method studies: the benchmarks
against EUROCONTROL reference data, the parameter sweeps, and the scripts that
regenerate the figures of each study paper (`regenerate_*.py`,
`build_*_paper.py`). They are research code. They are **not** part of the
`opdi` package: `pip install opdi` does not install them, and the package never
imports from here. The scripts import the package and run its real pipeline
code, so a study measures what the pipeline does.

Most scripts need the OpenSky cluster and its S3 storage, and some need
reference data from EUROCONTROL's PRISME warehouse. Their tests, which run on
a laptop with a local Spark session, are in `tests/benchmarks/`.

## Study variants

The package keeps two behaviours: today's default configuration (release
v2.0.0) and the configuration that reproduces the published v0.0.2 release.
The other variants that the studies compared live in `study_variants/`:

| Module | What it holds |
|---|---|
| `study_variants/segmentation.py` | The track segmentation rules `no_month_suffix`, `traffic_style`, `airframe_only`, `ground_anchored`, `airport_anchored`, `vertical_profile`, `recommended` and `debounced`, and the four thresholds only they use (`StudySegmentationParams`). |
| `study_variants/detection.py` | The flight-list mode `nearest` and the `trend_ooa` option (`StudyFlightListProcessor`, `StudyDetectionConfig`). |

A script turns a variant on explicitly. For a segmentation rule:

```python
from study_variants.segmentation import register_study_arms

register_study_arms()                        # adds the rules to the pipeline's table
config.segmentation.method = "debounced"     # now accepted by step 02
```

Without the call, the package refuses the name and says where the rule lives.
For the flight list:

```python
from study_variants.detection import StudyDetectionConfig, StudyFlightListProcessor

config.detection = StudyDetectionConfig(trend_ooa=True)
StudyFlightListProcessor(spark, config).process_dai(month, adep_mode="nearest",
                                                    ades_mode="nearest")
```

## Where the scripts read and write

`OPDIConfig.for_environment("opensky")` puts a release's tables in a folder
named after the release (`<warehouse>/v2.0.0`). The study data was built before
that folder existed and sits directly in the warehouse (for example
`s3a://eurocontrol/opdi/research/...`). Every script that reads it therefore
sets `config.project.release = ""`.
