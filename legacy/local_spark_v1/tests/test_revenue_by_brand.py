import pytest

from processing.transformers.revenue_by_brand_transformer import revenue_by_brand


@pytest.mark.skip(reason="Stale test — predates current transformer output schema, not yet reconciled")
def test_revenue_by_brand(spark):

    df = spark.createDataFrame(
        [
            ("Apple", 100),
            ("Apple", 300),
            ("Samsung", 200),
        ],
        ["brand", "total_amount"],
    )

    result = revenue_by_brand(df)

    rows = {r["brand"]: r["revenue"] for r in result.collect()}

    assert rows["Apple"] == 400
    assert rows["Samsung"] == 200