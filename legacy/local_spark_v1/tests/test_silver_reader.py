from processing.readers.silver_reader import read_silver_stream
import pytest

@pytest.mark.skip(reason="Stale test — requires a pre-existing Delta table at SILVER_PATH, not set up in this test env")
def test_silver_reader(spark):

    df = read_silver_stream(spark)

    assert df.isStreaming