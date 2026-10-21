"""Shared local Spark test fixture."""

from __future__ import annotations

import os
import sys

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    from pathlib import Path

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # On Windows, if HADOOP_HOME is not set but temp/hadoop exists, configure it
    if sys.platform.startswith("win") and not os.environ.get("HADOOP_HOME"):
        temp_hadoop = Path(os.environ.get("TEMP", "C:/Temp")) / "hadoop"
        if (temp_hadoop / "bin" / "winutils.exe").exists():
            os.environ["HADOOP_HOME"] = str(temp_hadoop)
            os.environ["PATH"] = str(temp_hadoop / "bin") + os.pathsep + os.environ.get("PATH", "")
    session = (
        SparkSession.builder.appName("f1_pipeline_tests")
        .master("local[1]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )
    yield session
    session.stop()
