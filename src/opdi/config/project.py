"""Where the pipeline keeps its tables: :class:`ProjectConfig`."""

from dataclasses import dataclass

from opdi.output.schema import RELEASE


@dataclass
class ProjectConfig:
    """Where the pipeline's tables are kept."""

    project_name: str = "project_opdi"
    """Name of the catalog database that holds the tables (``local``
    environment), or ``"opensky"`` for plain Parquet folders."""

    warehouse_path: str = "./data/warehouse"
    """Folder (local path or ``s3a://`` address) under which the tables are
    written. :meth:`OPDIConfig.for_environment` sets it for each environment."""

    release: str = RELEASE
    """Name of the data release, for example ``v2.0.0``.

    In the ``opensky`` environment every table, the progress records and the
    published files of a release are kept in a folder of this name inside the
    warehouse (see :attr:`release_root`), so two releases never share a
    table. Set it to an empty string to use the warehouse path itself, for
    example to read a folder that holds its tables directly."""

    @property
    def release_root(self) -> str:
        """The folder that holds this release's tables.

        In the ``opensky`` environment it is ``<warehouse_path>/<release>``,
        for example ``s3a://eurocontrol/opdi/v2.0.0``. In the other
        environments, and when :attr:`release` is empty, it is
        :attr:`warehouse_path` itself.
        """
        if self.project_name == "opensky" and self.release:
            return f"{self.warehouse_path.rstrip('/')}/{self.release}"
        return self.warehouse_path
