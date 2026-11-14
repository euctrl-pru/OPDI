"""What OPDI publishes, and how.

* :mod:`opdi.output.schema`: the declared columns of the published files.
* :mod:`opdi.output.published`: step 09, which writes the daily flight list
  and flight events files.

``import opdi.output.schema`` works without PySpark installed.
"""

#: Names of the monthly exporters of release v0.0.2 (steps 05 and 06), which
#: are not part of this package; importing them raises an error that says so.
_REMOVED = ("CSVExporter", "ParquetExporter", "clean_and_save_data")


def __getattr__(name):
    if name in _REMOVED:
        raise ImportError(
            f"{name} was the monthly v0.0.2 export (steps 05 and 06), which OPDI no "
            f"longer produces; the daily files are written by opdi.output.published "
            f"(step 09)"
        )
    raise AttributeError(f"module 'opdi.output' has no attribute {name!r}")


__all__ = []
