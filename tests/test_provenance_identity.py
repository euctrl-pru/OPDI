"""An emptied S3 prefix must read as absent, not as a populated table.

MinIO leaves a zero-byte key ending in `/` behind when a prefix is emptied.
`s3_identity` used to count it, so `Stage.stale` saw one object, called the
table present, and the chain skipped rebuilding it -- a deleted table reporting
itself current. Both halves of that are silent: no error, and a manifest entry
that says everything is fine.

Tested against a stubbed pageinator rather than S3, so it runs without
credentials.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "benchmarks"))

import datetime as dt

import pytest

import provenance


class _FakeS3:
    def __init__(self, contents):
        self._contents = contents

    def get_paginator(self, _name):
        contents = self._contents

        class _Paginator:
            def paginate(self, **_kw):
                return [{"Contents": contents}]

        return _Paginator()


def _obj(key, size):
    return {"Key": key, "Size": size,
            "LastModified": dt.datetime(2026, 8, 22, 12, 0, 0)}


@pytest.fixture
def patched(monkeypatch):
    def install(contents):
        fake = _FakeS3(contents)
        monkeypatch.setitem(
            sys.modules, "boto3", type("m", (), {"client": lambda *a, **k: fake})
        )
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "y")
    return install


def test_a_prefix_holding_only_a_directory_marker_is_empty(patched):
    """The regression. One zero-byte key ending in `/` is not a table."""
    patched([_obj("opdi/research/trend_votes_agl/", 0)])
    got = provenance.s3_identity("s3a://eurocontrol/opdi/research/trend_votes_agl")
    assert got["objects"] == 0
    assert got["bytes"] == 0


def test_real_objects_are_counted(patched):
    patched([
        _obj("opdi/research/trend_votes_agl/", 0),          # marker, ignored
        _obj("opdi/research/trend_votes_agl/part-0.parquet", 1024),
        _obj("opdi/research/trend_votes_agl/part-1.parquet", 2048),
        _obj("opdi/research/trend_votes_agl/_SUCCESS", 0),  # real, zero bytes
    ])
    got = provenance.s3_identity("s3a://eurocontrol/opdi/research/trend_votes_agl")
    # Three: the two parts and _SUCCESS. _SUCCESS is zero bytes but does not
    # end in `/`, and it is a genuine artefact of a committed write -- dropping
    # it would make a successful empty write indistinguishable from no write.
    assert got["objects"] == 3
    assert got["bytes"] == 3072
