from processing.transformers.customer_metrics_transformer import build_customer_metrics


def test_customer_metrics(spark):

    df = spark.createDataFrame(
        [
            ("C1", 100),
            ("C1", 200),
            ("C2", 500),
        ],
        ["customer_id", "total_amount"],
    )

    result = build_customer_metrics(df)

    rows = {r["customer_id"]: r["lifetime_value"] for r in result.collect()}

    assert rows["C1"] == 300
    assert rows["C2"] == 500