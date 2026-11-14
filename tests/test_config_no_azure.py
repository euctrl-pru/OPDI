"""The configuration carries nothing of the removed Azure environments."""

import dataclasses

from opdi.config import OPDIConfig


def test_no_azure_or_kerberos_setting_is_left():
    for env in ("local", "opensky"):
        config = OPDIConfig.for_environment(env)
        config.spark.enable_iceberg = True
        keys = config.spark.to_spark_config(config.project)
        assert not [k for k in keys if "azure" in k or "kerberos" in k], env
    assert "abfs://" not in OPDIConfig().project.warehouse_path
    fields = {f.name for f in dataclasses.fields(OPDIConfig().project)}
    assert "hadoop_filesystem" not in fields
