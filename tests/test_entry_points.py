"""The commands documented for operators work from the repository root.

``python -m opdi.orchestration ...`` is what the Airflow tasks run. It must
work from a checkout of the repository as well as from an installed package,
so nothing at the root of the repository may be named ``opdi``.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _run(*args):
    env = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
    return subprocess.run(
        [sys.executable, *args], cwd=ROOT, capture_output=True, text=True, env=env, timeout=120
    )


def test_nothing_at_the_repository_root_is_named_opdi():
    assert not (ROOT / "opdi.py").exists()
    assert not (ROOT / "opdi").exists()


def test_the_orchestration_module_runs_from_the_repository_root():
    out = _run("-m", "opdi.orchestration", "plan", "--start", "2025-01-01", "--end", "2025-01-02")
    assert out.returncode == 0, out.stderr
    assert json.loads(out.stdout) == ["2025-01-02", "2025-01-01"]


def test_python_dash_m_opdi_shows_the_range_runner_help():
    out = _run("-m", "opdi", "--help")
    assert out.returncode == 0, out.stderr
    assert "--start" in out.stdout and "--europe" in out.stdout


def test_the_other_modules_with_a_command_line_run_from_the_repository_root():
    for module in ("opdi.periodrun", "opdi.sql.render", "opdi.cli"):
        out = _run("-m", module, "--help")
        assert out.returncode == 0, (module, out.stderr)
        assert "usage" in out.stdout.lower(), module
