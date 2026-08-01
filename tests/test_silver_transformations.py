from processing.transformers.silver_transformer import clean_orders


def test_clean_orders(spark):

    df = spark.createDataFrame(
        [
            ("O1", "C1", "P1", 100.0),
            ("O2", None, "P2", 100.0),
            ("O3", "C3", "P3", -50.0),
            ("O1", "C1", "P1", 100.0),
        ],
        [
            "order_id",
            "customer_id",
            "product_id",
            "total_amount",
        ],
    )

    cleaned = clean_orders(df)

    assert cleaned.count() == 1