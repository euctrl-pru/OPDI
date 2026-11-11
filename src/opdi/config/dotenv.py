"""Reading a ``.env`` file into the environment when the configuration is imported."""

import os
from pathlib import Path


def _load_dotenv() -> None:
    """Read ``KEY=value`` lines of a ``.env`` file into the environment.

    The file is looked for in the working directory, then in the repository
    root. A variable already set in the environment is not replaced.
    """
    for candidate in [Path.cwd(), Path(__file__).resolve().parent.parent.parent]:
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
