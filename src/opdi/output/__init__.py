"""Output modules for exporting OPDI data.

The names below are imported only when first used, so that
``import opdi.output.schema`` works without PySpark installed.
"""

import importlib

_LAZY = {
    "CSVExporter": "opdi.output.csv_exporter",
    "clean_and_save_data": "opdi.output.csv_exporter",
    "ParquetExporter": "opdi.output.parquet_exporter",
}


def __getattr__(name):
    if name in _LAZY:
        value = getattr(importlib.import_module(_LAZY[name]), name)
        globals()[name] = value
        return value
    raise AttributeError(f"module 'opdi.output' has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | set(_LAZY))


__all__ = [
    "CSVExporter",
    "ParquetExporter",
    "clean_and_save_data",
]
