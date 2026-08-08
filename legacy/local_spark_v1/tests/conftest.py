import pytest

from processing.spark_session import create_spark_session


@pytest.fixture(scope="session")
def spark():
    spark = create_spark_session()
    yield spark
    spark.stop()