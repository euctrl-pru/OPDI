"""
Spark session management utilities for OPDI pipeline.

Provides centralized Spark session creation with consistent configuration
across all pipeline components.
"""

import os
from typing import Dict, Optional
from pyspark.sql import SparkSession
from opdi.config import OPDIConfig, SparkConfig, ProjectConfig


class SparkSessionManager:
    """
    Factory for creating and managing Spark sessions with OPDI configurations.

    This class centralizes all Spark configuration logic, eliminating duplication
    across pipeline scripts and enabling environment-specific settings.
    """

    @staticmethod
    def create_session(
        app_name: str = "OPDI Pipeline",
        config: Optional[OPDIConfig] = None,
        env: str = "dev",
        extra_configs: Optional[Dict[str, str]] = None,
        distributed: bool = False,
        container_image: Optional[str] = None,
    ) -> SparkSession:
        """
        Create a new Spark session with OPDI configuration.

        Args:
            app_name: Name for the Spark application
            config: OPDI configuration object. If None, creates config for specified env
            env: Environment name ("dev", "live", "local", "opensky") - used if config is None
            extra_configs: Additional Spark configurations to override defaults
            distributed: If True, enable Kubernetes distributed mode (opensky env)
            container_image: Override K8s executor container image (e.g. "my-registry/opdi-spark:v4.1.1")

        Returns:
            Configured SparkSession

        Example:
            >>> from opdi.utils.spark_helpers import SparkSessionManager
            >>> spark = SparkSessionManager.create_session("Track Processing", env="live")
            >>> df = spark.table("project_opdi.osn_statevectors_v2")
        """
        if config is None:
            config = OPDIConfig.for_environment(env)

        config.spark.app_name = app_name

        if container_image:
            config.spark.k8s_container_image = container_image

        spark_configs = config.spark.to_spark_config(config.project)

        if distributed:
            spark_configs.update(config.spark.to_distributed_config())

        if extra_configs:
            spark_configs.update(extra_configs)

        builder = SparkSession.builder.appName(app_name)

        if distributed and config.spark.k8s_master:
            builder = builder.master(config.spark.k8s_master)

        for key, value in spark_configs.items():
            builder = builder.config(key, value)

        if config.spark.enable_hive:
            builder = builder.enableHiveSupport()

        return builder.getOrCreate()

    @staticmethod
    def get_or_create(
        app_name: str = "OPDI Pipeline",
        config: Optional[OPDIConfig] = None,
        env: str = "dev",
        distributed: bool = False,
    ) -> SparkSession:
        """
        Get existing Spark session or create new one if none exists.

        Args:
            app_name: Name for the Spark application
            config: OPDI configuration object. If None, creates config for specified env
            env: Environment name ("dev", "live", "local", "opensky")
            distributed: If True, enable Kubernetes distributed mode (opensky env)

        Returns:
            Active SparkSession (existing or newly created)

        Example:
            >>> spark = SparkSessionManager.get_or_create("My Analysis")
        """
        try:
            spark = SparkSession.getActiveSession()
            if spark is not None:
                return spark
        except Exception:
            pass

        return SparkSessionManager.create_session(app_name, config, env, distributed=distributed)

    @staticmethod
    def stop_session(spark: SparkSession) -> None:
        """
        Stop the given Spark session and clean up resources.

        Args:
            spark: SparkSession to stop

        Example:
            >>> spark = SparkSessionManager.create_session()
            >>> # ... do work ...
            >>> SparkSessionManager.stop_session(spark)
        """
        if spark is not None:
            spark.stop()

    @staticmethod
    def create_local_session(
        app_name: str = "OPDI Local",
        master: str = "local[*]",
        extra_configs: Optional[Dict[str, str]] = None,
    ) -> SparkSession:
        """
        Create a lightweight Spark session for local testing.

        This creates a minimal Spark session without Iceberg, Hive, or Azure dependencies,
        suitable for unit testing and local development.

        Args:
            app_name: Name for the Spark application
            master: Spark master URL (default: "local[*]" uses all cores)
            extra_configs: Additional Spark configurations

        Returns:
            Local SparkSession for testing

        Example:
            >>> spark = SparkSessionManager.create_local_session("Unit Tests")
            >>> df = spark.createDataFrame([(1, "test")], ["id", "name"])
        """
        builder = SparkSession.builder.appName(app_name).master(master)

        # Minimal configuration for local testing
        default_configs = {
            "spark.driver.memory": "2G",
            "spark.executor.memory": "2G",
            "spark.sql.shuffle.partitions": "4",
            "spark.ui.showConsoleProgress": "false",
        }

        # Apply default configs
        for key, value in default_configs.items():
            builder = builder.config(key, value)

        # Apply extra configs if provided
        if extra_configs:
            for key, value in extra_configs.items():
                builder = builder.config(key, value)

        return builder.getOrCreate()


def get_spark(
    env: str = "dev",
    app_name: str = "OPDI Pipeline",
    distributed: bool = False,
    container_image: Optional[str] = None,
) -> SparkSession:
    """
    Convenience function to get a Spark session with OPDI configuration.

    This is a shorthand for SparkSessionManager.create_session().

    Args:
        env: Environment name ("dev", "live", "local", "opensky")
        app_name: Name for the Spark application
        distributed: If True, enable Kubernetes distributed mode (opensky env)
        container_image: Override K8s executor container image (e.g. "my-registry/opdi-spark:v4.1.1")

    Returns:
        Configured SparkSession

    Example:
        >>> from opdi.utils.spark_helpers import get_spark
        >>> spark = get_spark("opensky", distributed=True, container_image="my-registry/opdi-spark:v4.1.1")
    """
    return SparkSessionManager.create_session(
        app_name=app_name, env=env, distributed=distributed, container_image=container_image,
    )
