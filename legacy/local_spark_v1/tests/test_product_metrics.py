from processing.transformers.product_metrics_transformer import build_product_metrics


def test_product_metrics(spark):

    df = spark.createDataFrame(
        [
            ("P1", 2),
            ("P1", 3),
            ("P2", 1),
        ],
        ["product_id", "quantity"],
    )

    result = build_product_metrics(df)

    rows = {r["product_id"]: r["units_sold"] for r in result.collect()}

    assert rows["P1"] == 5
    assert rows["P2"] == 1