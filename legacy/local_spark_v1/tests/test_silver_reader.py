from processing.readers.silver_reader import read_silver_stream


def test_silver_reader(spark):

    df = read_silver_stream(spark)

    assert df.isStreaming