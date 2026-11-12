import pytest

from local_pipeline.config import LocalConfig
from local_pipeline.spark_session import create_spark_session


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """A plain local Spark session (Parquet tables, nothing to download)."""
    config = LocalConfig(data_dir=str(tmp_path_factory.mktemp("unit")))
    session = create_spark_session(config, delta=False)
    yield session
    session.stop()
