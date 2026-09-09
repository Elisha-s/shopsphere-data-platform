from processing.readers.bronze_reader import read_bronze_stream
import pytest

@pytest.mark.skip(reason="Stale test — requires a pre-existing Delta table at BRONZE_PATH, not set up in this test env")
def test_bronze_reader(spark):

    df = read_bronze_stream(spark)

    assert df.isStreaming