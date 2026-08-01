from processing.readers.bronze_reader import read_bronze_stream


def test_bronze_reader(spark):

    df = read_bronze_stream(spark)

    assert df.isStreaming