"""
Spark session management utilities for OPDI pipeline.

Provides centralized Spark session creation with consistent configuration
across all pipeline components.
"""

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
    ) -> SparkSession:
        """
        Create a new Spark session with OPDI configuration.

        Args:
            app_name: Name for the Spark application
            config: OPDI configuration object. If None, creates config for specified env
            env: Environment name ("dev", "live", "local") - used if config is None
            extra_configs: Additional Spark configurations to override defaults

        Returns:
            Configured SparkSession with Hive support enabled

        Example:
            >>> from opdi.utils.spark_helpers import SparkSessionManager
            >>> spark = SparkSessionManager.create_session("Track Processing", env="live")
            >>> df = spark.table("project_opdi.osn_statevectors_v2")
        """
        # Get configuration
        if config is None:
            config = OPDIConfig.for_environment(env)

        # Override app name in spark config
        config.spark.app_name = app_name

        # Get Spark configuration dictionary
        spark_configs = config.spark.to_spark_config(config.project)

        # Apply extra configs if provided
        if extra_configs:
            spark_configs.update(extra_configs)

        # Build Spark session
        builder = SparkSession.builder.appName(app_name)

        # Apply all configurations
        for key, value in spark_configs.items():
            builder = builder.config(key, value)

        # Enable Hive support and create session
        spark = builder.enableHiveSupport().getOrCreate()

        return spark

    @staticmethod
    def get_or_create(
        app_name: str = "OPDI Pipeline",
        config: Optional[OPDIConfig] = None,
        env: str = "dev",
    ) -> SparkSession:
        """
        Get existing Spark session or create new one if none exists.

        This method is useful when you want to reuse an existing session
        within the same application context.

        Args:
            app_name: Name for the Spark application
            config: OPDI configuration object. If None, creates config for specified env
            env: Environment name ("dev", "live", "local")

        Returns:
            Active SparkSession (existing or newly created)

        Example:
            >>> spark = SparkSessionManager.get_or_create("My Analysis")
        """
        try:
            # Try to get active session
            spark = SparkSession.getActiveSession()
            if spark is not None:
                return spark
        except Exception:
            pass

        # No active session, create new one
        return SparkSessionManager.create_session(app_name, config, env)

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


def get_spark(env: str = "dev", app_name: str = "OPDI Pipeline") -> SparkSession:
    """
    Convenience function to get a Spark session with OPDI configuration.

    This is a shorthand for SparkSessionManager.create_session().

    Args:
        env: Environment name ("dev", "live", "local")
        app_name: Name for the Spark application

    Returns:
        Configured SparkSession

    Example:
        >>> from opdi.utils.spark_helpers import get_spark
        >>> spark = get_spark("live", "Flight Events Processing")
    """
    return SparkSessionManager.create_session(app_name=app_name, env=env)
