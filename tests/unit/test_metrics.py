from shopsphere_data_platform_etl.core.metrics import customer_metrics, product_metrics

SCHEMA = (
    "order_id string, customer_id string, product_id string, product_name string, category string, brand string, "
    "quantity int, unit_price double, total_amount double, order_status string, order_timestamp string"
)

ORDERS = [
    ("o1", "c1", "p1", "Dell Laptop", "Laptop", "Dell", 1, 100.0, 100.0, "PAID", "2026-01-01 10:00:00"),
    ("o2", "c1", "p2", "Sony Headphones", "Headphones", "Sony", 2, 50.0, 100.0, "DELIVERED", "2026-01-02 10:00:00"),
    ("o3", "c1", "p1", "Dell Laptop", "Laptop", "Dell", 1, 100.0, 100.0, "CREATED", "2026-01-03 10:00:00"),
    ("o4", "c2", "p1", "Dell Laptop", "Laptop", "Dell", 3, 100.0, 300.0, "SHIPPED", "2026-01-01 12:00:00"),
    ("o5", "c2", "p2", "Sony Headphones", "Headphones", "Sony", 1, 50.0, 50.0, "CANCELLED", "2026-01-04 10:00:00"),
]


def current_orders(spark):
    return (
        spark.createDataFrame(ORDERS, SCHEMA)
        .withColumnRenamed("order_timestamp", "ts")
        .selectExpr("*", "cast(ts as timestamp) as order_timestamp")
        .drop("ts")
    )


def test_customer_metrics_count_only_paid_for_orders(spark):
    rows = {row["customer_id"]: row for row in customer_metrics(current_orders(spark)).collect()}
    assert rows["c1"]["total_orders"] == 2  # the CREATED order is not revenue yet
    assert rows["c1"]["lifetime_value"] == 200.0
    assert rows["c1"]["average_order_value"] == 100.0
    assert rows["c1"]["unique_products_purchased"] == 2
    assert str(rows["c1"]["last_purchase_at"]) == "2026-01-02 10:00:00"
    assert rows["c2"]["total_orders"] == 1  # the CANCELLED order is excluded
    assert rows["c2"]["lifetime_value"] == 300.0


def test_product_metrics(spark):
    rows = {row["product_id"]: row for row in product_metrics(current_orders(spark)).collect()}
    assert rows["p1"]["units_sold"] == 4
    assert rows["p1"]["total_revenue"] == 400.0
    assert rows["p1"]["total_orders"] == 2
    assert rows["p1"]["unique_customers"] == 2
    assert rows["p2"]["units_sold"] == 2
    assert rows["p2"]["brand"] == "Sony"


def test_gold_totals_agree_with_each_other(spark):
    orders = current_orders(spark)
    by_customer = customer_metrics(orders).agg({"lifetime_value": "sum"}).first()[0]
    by_product = product_metrics(orders).agg({"total_revenue": "sum"}).first()[0]
    assert by_customer == by_product == 500.0
