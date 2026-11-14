"""The Spark settings measured on the cluster reach the cluster session only."""

from opdi.config import OPDIConfig
from opdi.config.environments import OPENSKY_CLUSTER_SETTINGS

KEPT = {
    "spark.sql.shuffle.partitions": "104",
    "spark.storage.replication.proactive": "true",
    "spark.task.maxFailures": "8",
    "spark.stage.maxConsecutiveAttempts": "8",
    "spark.hadoop.fs.s3a.retry.throttle.interval": "1s",
    "spark.hadoop.fs.s3a.connection.request.timeout": "120s",
}


def test_the_cluster_session_has_the_measured_settings():
    spark = OPDIConfig.for_environment("opensky").spark
    distributed = spark.to_distributed_config()
    for key, value in KEPT.items():
        assert distributed[key] == value
    assert set(OPENSKY_CLUSTER_SETTINGS) <= set(distributed)


def test_the_executors_fit_the_namespace_quota():
    # 30 CPUs and 192 GiB, of which the driver's pod takes 4 CPUs and 16 GiB.
    spark = OPDIConfig.for_environment("opensky").spark
    distributed = spark.to_distributed_config()
    n = int(distributed["spark.executor.instances"])
    cores = int(distributed["spark.kubernetes.executor.limit.cores"])

    def mib(size):
        size = size.lower()
        return int(size[:-1]) * (1024 if size.endswith("g") else 1)

    pod = mib(distributed["spark.executor.memory"]) + mib(
        distributed["spark.executor.memoryOverhead"]
    )
    assert (n, cores, pod) == (13, 2, 13824)
    assert distributed["spark.executor.memory"] == "11g"
    assert n * cores <= 30 - 4
    assert n * pod <= (192 - 16) * 1024


def test_a_session_on_one_machine_gets_none_of_them():
    for env in ("local", "opensky"):
        cfg = OPDIConfig.for_environment(env)
        local = cfg.spark.to_spark_config(cfg.project)
        assert not set(OPENSKY_CLUSTER_SETTINGS) & set(local)
    local_cfg = OPDIConfig.for_environment("local").spark
    assert local_cfg.cluster_settings == {}
    assert local_cfg.to_distributed_config() == {}
