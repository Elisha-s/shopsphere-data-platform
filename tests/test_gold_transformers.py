from processing.transformers.revenue_by_category_transformer import revenue_by_category
from processing.transformers.revenue_by_brand_transformer import revenue_by_brand
from processing.transformers.customer_metrics_transformer import customer_metrics
from processing.transformers.sales_summary_transformer import sales_summary


def sample_df(spark):

    data = [

        ("Electronics","Dell","C001",1000),

        ("Electronics","Dell","C001",500),

        ("Electronics","HP","C002",800),

        ("Furniture","Ikea","C003",700)

    ]

    return spark.createDataFrame(
        data,
        [
            "category",
            "brand",
            "customer_id",
            "total_amount"
        ]
    )


def test_revenue_by_category(spark):

    df = revenue_by_category(sample_df(spark))

    electronics = (
        df.filter(df.category=="Electronics")
        .first()
    )

    assert electronics.revenue == 2300


def test_revenue_by_brand(spark):

    df = revenue_by_brand(sample_df(spark))

    dell = (
        df.filter(df.brand=="Dell")
        .first()
    )

    assert dell.revenue == 1500


def test_customer_metrics(spark):

    df = customer_metrics(sample_df(spark))

    c1 = (
        df.filter(df.customer_id=="C001")
        .first()
    )

    assert c1.total_spend == 1500


def test_sales_summary(spark):

    df = sales_summary(sample_df(spark))

    row = df.first()

    assert row.total_orders == 4

    assert row.total_revenue == 3000