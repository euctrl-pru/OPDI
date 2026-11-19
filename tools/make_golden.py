"""Write the reference files of the golden-output test.

Runs ``tests/test_golden_outputs.py`` with ``OPDI_WRITE_GOLDEN=1``, which makes
it write every output of the synthetic day to ``tests/golden/`` instead of
comparing. Run it from the repository root, on code whose outputs are known to
be right, and commit the files it writes:

    .venv310/bin/python tools/make_golden.py
"""

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    env = dict(os.environ, OPDI_WRITE_GOLDEN="1")
    return subprocess.call(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/test_golden_outputs.py",
            "-q",
            "-p",
            "no:cacheprovider",
        ],
        cwd=ROOT,
        env=env,
    )


if __name__ == "__main__":
    sys.exit(main())
