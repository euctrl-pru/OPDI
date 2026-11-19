"""Pipeline variants that only the study scripts use.

The ``opdi`` package keeps two behaviours: today's default configuration
(release v2.0.0) and the configuration that reproduces the published v0.0.2
release. The variants the studies compared them with live here, so the study
scripts under ``benchmarks/`` can still run them through the real pipeline.

* :mod:`study_variants.segmentation`: the other track segmentation rules.
  Call ``register_study_arms()`` before selecting one by name.
* :mod:`study_variants.detection`: the ``nearest`` flight-list mode and the
  ``trend_ooa`` option.

The package never imports from here.
"""
