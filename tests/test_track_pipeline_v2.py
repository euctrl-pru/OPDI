import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "benchmarks"))

import track_pipeline_v2 as tp  # noqa: E402


def test_every_table_step_03_writes_is_redirected():
    """The guard catches an unredirected write; the map must make it unnecessary.

    `opdi_endpoint_candidates` is written mode="overwrite". Unredirected it
    resolves to the production cache, so an unguarded run destroys it and every
    later arm reads another arm's candidates. The smoke run proved the guard
    fires; this proves it should not have to.
    """
    assert "opdi_endpoint_candidates" in tp.TABLES
    assert "opdi_flight_list" in tp.TABLES
    resolved = tp.table_for("legacy", "opdi_endpoint_candidates")
    assert resolved.startswith("research/tcv2/legacy/")
