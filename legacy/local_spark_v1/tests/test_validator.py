from pyspark.sql import Row

from processing.quality.validator import split_valid_invalid


def test_split_valid_invalid(spark):

    data = [
        Row(
            order_id="O1",
            customer_id="C1",
            product_id="P1",
            total_amount=100.0
        ),

        Row(
            order_id=None,
            customer_id="C2",
            product_id="P2",
            total_amount=200.0
        ),

        Row(
            order_id="O3",
            customer_id="C3",
            product_id="P3",
            total_amount=-50.0
        )
    ]

    df = spark.createDataFrame(data)

    valid, invalid = split_valid_invalid(df)

    assert valid.count() == 1
    assert invalid.count() == 2