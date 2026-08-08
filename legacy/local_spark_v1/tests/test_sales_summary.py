from processing.transformers.sales_summary_transformer import sales_summary


def test_sales_summary(spark):

    df = spark.createDataFrame(
        [
            (100.0,),
            (200.0,),
            (300.0,),
        ],
        ["total_amount"],
    )

    result = sales_summary(df)

    row = result.collect()[0]

    assert row["total_revenue"] == 600.0
    assert row["total_orders"] == 3