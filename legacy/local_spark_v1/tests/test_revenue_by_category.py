from processing.transformers.revenue_by_category_transformer import revenue_by_category


def test_revenue_by_category(spark):

    df = spark.createDataFrame(
        [
            ("Laptop", 100),
            ("Laptop", 200),
            ("Phone", 300),
        ],
        ["category", "total_amount"],
    )

    result = revenue_by_category(df)

    rows = {r["category"]: r["revenue"] for r in result.collect()}

    assert rows["Laptop"] == 300
    assert rows["Phone"] == 300