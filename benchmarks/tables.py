"""
One place that decides which warehouse the benchmarks read.

The pipeline resolves every table against ``config.project.warehouse_path``, so
a single environment variable can run it into a parallel prefix. The benchmarks
had their table paths written out as literals, which meant an isolated pipeline
run would write to one place while the sweeps kept reading another -- and the
resulting numbers would look perfectly ordinary.

``OPDI_WAREHOUSE`` sets both. Unset, everything points where it always did.

Ground truth is the deliberate exception. ``research/reference`` is extracted by
the ``eurocontrol`` R package against the PRISME Oracle warehouse, which no run
on this cluster can reach; it is an external input, not a pipeline product, so
"from scratch" cannot apply to it and isolating it would only hide it. It stays
absolute, and the report names it as the boundary of reproducibility.
"""

import os

#: Root every pipeline table is resolved against.
WAREHOUSE = os.environ.get("OPDI_WAREHOUSE", "s3a://eurocontrol/opdi")

#: Never isolated -- see the module docstring.
PRODUCTION_ROOT = "s3a://eurocontrol/opdi"


def table(name: str) -> str:
    """Path to a pipeline table under the active warehouse."""
    return f"{WAREHOUSE}/{name}"


def fixed(name: str) -> str:
    """Path to something that is an input to every run, isolated or not."""
    return f"{PRODUCTION_ROOT}/{name}"


def describe() -> str:
    return ("warehouse=" + WAREHOUSE
            + ("" if WAREHOUSE == PRODUCTION_ROOT else "  (ISOLATED)"))
