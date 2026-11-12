from shopsphere_data_platform_etl.core.quality import ORDER_RULES, invalid_orders, valid_orders

COLUMNS = (
    "event_id string, order_id string, customer_id string, product_id string, event_timestamp timestamp, "
    "quantity int, unit_price double, total_amount double, order_status string"
)


def order(**overrides):
    row = {
        "event_id": "e1",
        "order_id": "o1",
        "customer_id": "c1",
        "product_id": "p1",
        "event_timestamp": "2026-01-01 10:00:00",
        "quantity": 2,
        "unit_price": 50.0,
        "total_amount": 100.0,
        "order_status": "PAID",
    }
    row.update(overrides)
    return row


def frame(spark, rows):
    names = [part.split()[0] for part in COLUMNS.split(", ")]
    data = [tuple(row[name] for name in names) for row in rows]
    string_schema = COLUMNS.replace("event_timestamp timestamp", "event_timestamp string")
    return spark.createDataFrame(data, string_schema).selectExpr(
        *[f"cast({name} as timestamp) as {name}" if name == "event_timestamp" else name for name in names]
    )


def errors_for(spark, **overrides):
    rows = invalid_orders(frame(spark, [order(**overrides)])).collect()
    assert len(rows) == 1, "expected the row to be invalid"
    return set(rows[0]["validation_errors"].split(","))


def test_a_good_order_is_valid(spark):
    df = frame(spark, [order()])
    assert valid_orders(df).count() == 1
    assert invalid_orders(df).count() == 0


def test_each_broken_field_is_named(spark):
    assert errors_for(spark, event_id=None) == {"event_id_present"}
    assert errors_for(spark, order_id=None) == {"order_id_present"}
    assert errors_for(spark, customer_id=None) == {"customer_id_present"}
    assert errors_for(spark, product_id=None) == {"product_id_present"}
    assert errors_for(spark, event_timestamp=None) == {"event_timestamp_valid"}
    assert errors_for(spark, order_status="TELEPORTED") == {"order_status_known"}
    assert errors_for(spark, total_amount=150.0) == {"total_amount_consistent"}
    assert errors_for(spark, quantity=-2) == {"quantity_positive", "total_amount_consistent"}


def test_null_numbers_are_quarantined_not_lost(spark):
    """The original bug: a NULL made the row vanish from both tables."""
    assert "quantity_positive" in errors_for(spark, quantity=None)
    assert "unit_price_positive" in errors_for(spark, unit_price=None)
    assert "total_amount_positive" in errors_for(spark, total_amount=None)
    assert "order_status_known" in errors_for(spark, order_status=None)


def test_every_row_lands_in_exactly_one_table(spark):
    rows = [
        order(event_id="e1"),
        order(event_id="e2", quantity=None),
        order(event_id="e3", total_amount=None),
        order(event_id="e4", customer_id=None),
        order(event_id=None),
        order(event_id="e6", unit_price=None, order_status=None),
        order(event_id="e7", order_status="CANCELLED"),
    ]
    df = frame(spark, rows)
    assert valid_orders(df).count() + invalid_orders(df).count() == len(rows)
    assert valid_orders(df).count() == 2


def test_rules_never_evaluate_to_null(spark):
    df = frame(spark, [order(quantity=None, unit_price=None, total_amount=None, order_status=None, event_id=None)])
    for name, rule in ORDER_RULES.items():
        value = df.selectExpr(f"{rule} as result").first()["result"]
        assert value is not None, f"rule {name} evaluated to NULL"
