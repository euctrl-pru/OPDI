"""Reading a ``.env`` file into the environment when the configuration is imported."""

import os
from pathlib import Path

#: The repository root (``src/opdi/config/dotenv.py`` is three folders below it).
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]


def _load_dotenv() -> None:
    """Read ``KEY=value`` lines of a ``.env`` file into the environment.

    The file is looked for in the working directory, then in the repository
    root. A variable already set in the environment is not replaced.
    """
    for candidate in [Path.cwd(), REPOSITORY_ROOT]:
        env_file = candidate / ".env"
        if env_file.is_file():
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, _, value = line.partition("=")
                    os.environ.setdefault(key.strip(), value.strip())
            return


_load_dotenv()
