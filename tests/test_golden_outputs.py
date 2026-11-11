"""The pipeline's outputs on a fixed synthetic day must not change.

``tests/golden_pipeline.py`` runs steps 02 to 09 over one simulated day,
under today's default configuration and under the configuration that
reproduces the v0.0.2 release. This test compares every table and published
file the steps write with the reference copies in ``tests/golden/``: the same
columns, the same column types and the same values, row by row.

A difference means the change being tested alters what the pipeline
produces. When that is intended, write new reference files with
``python tools/make_golden.py`` and commit them with the change.
"""

import json
import math
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import golden_pipeline

GOLDEN_DIR = Path(__file__).resolve().parent / "golden"
CONFIGURATIONS = ("default", "v002")
FRAMES = golden_pipeline.OUTPUT_TABLES + tuple(
    f"published_{name}" for name in golden_pipeline.PUBLISHED_FILES
)
WRITE = os.environ.get("OPDI_WRITE_GOLDEN") == "1"


@pytest.fixture(scope="module")
def outputs(spark, tmp_path_factory):
    """Both golden runs, each in its own local warehouse."""
    result = {}
    for name in CONFIGURATIONS:
        warehouse = str(tmp_path_factory.mktemp(f"golden_{name}"))
        result[name] = golden_pipeline.run(spark, name, warehouse)
    if WRITE:
        for name, frames in result.items():
            folder = GOLDEN_DIR / name
            folder.mkdir(parents=True, exist_ok=True)
            for key, frame in frames.items():
                frame.to_parquet(folder / f"{key}.parquet", index=False)
                (folder / f"{key}.types.json").write_text(
                    json.dumps(frame.attrs["types"], indent=1) + "\n"
                )
    return result


def _plain(value):
    """A cell as plain Python values, so that two frames compare exactly."""
    if isinstance(value, np.ndarray):
        return [_plain(v) for v in value.tolist()]
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _plain(v) for k, v in sorted(value.items())}
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float):
        return "NaN" if math.isnan(value) else repr(value)
    if value is None or value is pd.NaT:
        return None
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    return value if isinstance(value, (bool, int, str)) else str(value)


def _canonical(frame: pd.DataFrame, tmp_path: Path, key: str) -> list:
    """Rows as text, after the same parquet round trip the reference went through."""
    path = tmp_path / f"{key}.parquet"
    frame.to_parquet(path, index=False)
    back = pd.read_parquet(path)
    return [json.dumps([_plain(v) for v in row], default=str)
            for row in back.itertuples(index=False, name=None)]


def test_the_runs_produce_rows(outputs):
    """The synthetic day is not empty: each run builds tracks, flights and events."""
    for name in CONFIGURATIONS:
        frames = outputs[name]
        assert len(frames["osn_tracks"]) > 0, name
        assert len(frames["opdi_flight_list"]) > 0, name
        assert len(frames["opdi_flight_events"]) > 0, name
        assert len(frames["published_flight_events"]) > 0, name


@pytest.mark.parametrize("key", FRAMES)
@pytest.mark.parametrize("name", CONFIGURATIONS)
def test_output_equals_the_reference(outputs, name, key, tmp_path):
    frame = outputs[name][key]
    reference_path = GOLDEN_DIR / name / f"{key}.parquet"
    types_path = GOLDEN_DIR / name / f"{key}.types.json"
    if WRITE:
        pytest.skip("writing the reference files")
    assert reference_path.exists(), (
        f"no reference file {reference_path}; run python tools/make_golden.py"
    )

    expected_types = [tuple(t) for t in json.loads(types_path.read_text())]
    assert [tuple(t) for t in frame.attrs["types"]] == expected_types

    reference = pd.read_parquet(reference_path)
    got = _canonical(frame, tmp_path, key)
    want = _canonical(reference, tmp_path, key + "_ref")
    assert len(got) == len(want), f"{name}/{key}: {len(got)} rows, reference has {len(want)}"
    differing = [i for i, (a, b) in enumerate(zip(got, want)) if a != b]
    assert not differing, (
        f"{name}/{key}: {len(differing)} of {len(got)} rows differ; first at row "
        f"{differing[0]}:\n  got  {got[differing[0]]}\n  want {want[differing[0]]}"
    )
