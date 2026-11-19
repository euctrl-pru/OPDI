"""The Spark settings measured on the cluster reach the cluster session only."""

from opdi.config import OPDIConfig
from opdi.config.environments import OPENSKY_CLUSTER_SETTINGS

KEPT = {
    "spark.sql.shuffle.partitions": "104",
    "spark.storage.replication.proactive": "true",
    "spark.task.maxFailures": "8",
    "spark.stage.maxConsecutiveAttempts": "8",
    "spark.excludeOnFailure.enabled": "true",
    "spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor": "1",
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


def test_a_failed_task_is_retried_on_another_executor():
    # One attempt per executor and task: the next attempt goes elsewhere, so
    # an executor that is dying cannot use up all the attempts of a task.
    distributed = OPDIConfig.for_environment("opensky").spark.to_distributed_config()
    assert distributed["spark.excludeOnFailure.enabled"] == "true"
    assert distributed["spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor"] == "1"
    for env in ("local", "opensky"):
        cfg = OPDIConfig.for_environment(env)
        local = cfg.spark.to_spark_config(cfg.project)
        assert not any(key.startswith("spark.excludeOnFailure") for key in local)


def test_a_session_on_one_machine_gets_none_of_them():
    for env in ("local", "opensky"):
        cfg = OPDIConfig.for_environment(env)
        local = cfg.spark.to_spark_config(cfg.project)
        assert not set(OPENSKY_CLUSTER_SETTINGS) & set(local)
    local_cfg = OPDIConfig.for_environment("local").spark
    assert local_cfg.cluster_settings == {}
    assert local_cfg.to_distributed_config() == {}


# --- Spark settings of one step ------------------------------------------------

STEP_PARTITIONS = {"02": "416"}


def test_the_track_steps_get_their_own_shuffle_partitions():
    spark = OPDIConfig.for_environment("opensky").spark
    for step, value in STEP_PARTITIONS.items():
        assert spark.settings_for_step(step) == {"spark.sql.shuffle.partitions": value}
    for step in ("01", "02a", "03", "04", "04b", "09"):
        assert spark.settings_for_step(step) == {}
    # The session itself keeps 104, the value measured for step 04.
    assert spark.to_distributed_config()["spark.sql.shuffle.partitions"] == "104"


def test_a_session_on_one_machine_gets_no_step_settings():
    for env in ("local", "opensky"):
        cfg = OPDIConfig.for_environment(env)
        cfg.spark.k8s_master = "" if env == "opensky" else cfg.spark.k8s_master
        for step in ("01", "02", "02a", "03", "04", "04b", "09"):
            assert cfg.spark.settings_for_step(step) == {}
    assert OPDIConfig.for_environment("local").spark.step_settings == {}


class _Conf:
    def __init__(self, values):
        self.values = dict(values)

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value):
        self.values[key] = value

    def unset(self, key):
        self.values.pop(key)


class _Session:
    def __init__(self, values):
        self.conf = _Conf(values)


def test_run_day_step_sets_the_step_value_and_puts_the_session_value_back(monkeypatch):
    from opdi import periodrun

    cfg = OPDIConfig.for_environment("opensky")
    spark = _Session({"spark.sql.shuffle.partitions": "104"})
    seen = {}

    def fake(spark_, config, step, day, kwargs):
        seen.setdefault(step, []).append(spark_.conf.get("spark.sql.shuffle.partitions"))
        if kwargs.get("fail"):
            raise RuntimeError("step failed")

    monkeypatch.setattr(periodrun, "_run_day_step", fake)
    periodrun.run_day_step(spark, cfg, "02", None, {})
    periodrun.run_day_step(spark, cfg, "04", None, {})
    try:
        periodrun.run_day_step(spark, cfg, "02", None, {"fail": True})
    except RuntimeError:
        pass
    periodrun.run_day_step(spark, cfg, "03", None, {})
    assert seen == {"02": ["416", "416"], "04": ["104"], "03": ["104"]}
    # Back to the session's value after each step, a failed one included.
    assert spark.conf.values == {"spark.sql.shuffle.partitions": "104"}
