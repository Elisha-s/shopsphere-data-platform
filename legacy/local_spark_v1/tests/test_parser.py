from processing.transformers.parser import parse_orders


def test_parser(spark):

    raw = spark.createDataFrame(
        [
            (
                '{"event_type":"ORDER_CREATED","order_id":"O1","customer_id":"C1","product_id":"P1","product_name":"Laptop","category":"Electronics","brand":"Dell","quantity":1,"unit_price":50000.0,"total_amount":50000.0,"payment_method":"UPI","order_status":"CREATED","city":"Bangalore","state":"Karnataka","country":"India","order_timestamp":"2026-01-01T10:00:00"}',
            )
        ],
        ["value"],
    )

    parsed = parse_orders(raw)

    assert parsed.count() == 1
    assert "order_id" in parsed.columns