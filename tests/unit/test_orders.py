import json

from shopsphere_data_platform_etl.core.orders import TRANSPORT_COLUMNS, dedup_key, flatten_orders, parse_events

TRANSPORT_SCHEMA = (
    "message_key string, raw_json string, kafka_topic string, kafka_partition int, kafka_offset long, "
    "enqueued_timestamp timestamp"
)


def event(**overrides):
    base = {
        "event_id": "e1",
        "event_type": "ORDER_CREATED",
        "event_version": "1.0",
        "event_timestamp": "2026-01-01T10:00:00.123456+00:00",
        "generated_at": "2026-01-01T10:00:00.123456+00:00",
        "is_late_event": False,
        "payload": {
            "order_id": "O-1",
            "customer_id": "C000001",
            "product_id": "P000001",
            "product_name": "Dell Laptop",
            "category": "Laptop",
            "brand": "Dell",
            "quantity": 2,
            "unit_price": 50000.0,
            "total_amount": 100000.0,
            "payment_method": "UPI",
            "order_status": "CREATED",
            "city": "Pune",
            "state": "Maharashtra",
            "country": "India",
            "order_timestamp": "2026-01-01T10:00:00.123456+00:00",
        },
    }
    base.update(overrides)
    return base


def bronze(spark, raw_values):
    rows = [(None, raw, "orders", 0, offset, None) for offset, raw in enumerate(raw_values)]
    transport = spark.createDataFrame(rows, TRANSPORT_SCHEMA).selectExpr(
        "message_key",
        "raw_json",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "current_timestamp() as enqueued_timestamp",
    )
    return parse_events(transport)


def test_bronze_keeps_raw_json_and_transport_metadata(spark):
    raw = json.dumps(event())
    row = bronze(spark, [raw]).first()
    assert row["raw_json"] == raw
    assert row["event_id"] == "e1"
    assert row["payload"]["order_id"] == "O-1"
    assert row["ingestion_timestamp"] is not None
    assert set(TRANSPORT_COLUMNS) <= set(row.asDict())


def test_malformed_json_still_reaches_bronze(spark):
    rows = bronze(spark, ["{this is not json", json.dumps(event())]).collect()
    assert len(rows) == 2
    broken = next(row for row in rows if row["raw_json"].startswith("{this"))
    assert broken["event_id"] is None


def test_flatten_produces_typed_columns(spark):
    row = flatten_orders(bronze(spark, [json.dumps(event())])).first()
    assert row["order_id"] == "O-1"
    assert row["quantity"] == 2
    assert row["total_amount"] == 100000.0
    assert row["event_timestamp"].year == 2026
    assert row["event_timestamp"].microsecond == 123456
    assert row["order_timestamp"] is not None


def test_bad_timestamp_becomes_null_instead_of_failing(spark):
    raw = json.dumps(event(event_timestamp="not-a-timestamp"))
    row = flatten_orders(bronze(spark, [raw])).first()
    assert row["event_timestamp"] is None


def test_dedup_key_is_the_event_id_when_there_is_one(spark):
    df = bronze(spark, [json.dumps(event(event_id="abc"))])
    assert df.select(dedup_key().alias("k")).first()["k"] == "abc"


def test_two_different_broken_messages_are_not_duplicates(spark):
    df = bronze(spark, ["{broken one", "{broken two", "{broken one"])
    keys = [row["k"] for row in df.select(dedup_key().alias("k")).collect()]
    assert None not in keys
    assert len(set(keys)) == 2
