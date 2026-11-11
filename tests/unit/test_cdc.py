from shopsphere_data_platform_etl.core.cdc import latest_change_per_order

SCHEMA = (
    "order_id string, event_id string, order_status string, event_timestamp timestamp, "
    "_change_type string, _commit_version long, _commit_timestamp timestamp"
)


def changes(spark, rows):
    data = [(o, e, s, ts, ct, v, "2026-01-01 00:00:00") for o, e, s, ts, ct, v in rows]
    string_schema = SCHEMA.replace("timestamp,", "string,").replace(
        "_commit_timestamp timestamp", "_commit_timestamp string"
    )
    return spark.createDataFrame(data, string_schema).selectExpr(
        "order_id",
        "event_id",
        "order_status",
        "cast(event_timestamp as timestamp) as event_timestamp",
        "_change_type",
        "_commit_version",
        "cast(_commit_timestamp as timestamp) as _commit_timestamp",
    )


def statuses(df):
    return {row["order_id"]: row["order_status"] for row in df.collect()}


def test_keeps_one_row_per_order(spark):
    df = changes(
        spark,
        [
            ("o1", "e1", "CREATED", "2026-01-01 10:00:00", "insert", 1),
            ("o1", "e2", "PAID", "2026-01-01 10:05:00", "insert", 2),
            ("o2", "e3", "CREATED", "2026-01-01 10:01:00", "insert", 1),
        ],
    )
    result = latest_change_per_order(df)
    assert result.count() == 2
    assert statuses(result) == {"o1": "PAID", "o2": "CREATED"}


def test_newest_means_event_time_not_commit_order(spark):
    """The original bug: a late event written in a later commit won."""
    df = changes(
        spark,
        [
            ("o1", "e2", "PAID", "2026-01-01 10:05:00", "insert", 1),
            # Happened earlier, but arrived (was committed) later.
            ("o1", "e1", "CREATED", "2026-01-01 10:00:00", "insert", 2),
        ],
    )
    assert statuses(latest_change_per_order(df)) == {"o1": "PAID"}


def test_ignores_preimages_and_deletes(spark):
    df = changes(
        spark,
        [
            ("o1", "e1", "CREATED", "2026-01-01 10:00:00", "insert", 1),
            ("o1", "e9", "OLD", "2026-01-01 11:00:00", "update_preimage", 2),
            ("o1", "e9", "GONE", "2026-01-01 12:00:00", "delete", 3),
            ("o1", "e2", "PAID", "2026-01-01 10:30:00", "update_postimage", 2),
        ],
    )
    assert statuses(latest_change_per_order(df)) == {"o1": "PAID"}


def test_change_feed_columns_are_removed(spark):
    df = changes(spark, [("o1", "e1", "CREATED", "2026-01-01 10:00:00", "insert", 1)])
    assert not [c for c in latest_change_per_order(df).columns if c.startswith("_")]


def test_same_timestamp_is_resolved_deterministically(spark):
    rows = [
        ("o1", "e-a", "PAID", "2026-01-01 10:00:00", "insert", 1),
        ("o1", "e-b", "SHIPPED", "2026-01-01 10:00:00", "insert", 1),
    ]
    assert statuses(latest_change_per_order(changes(spark, rows))) == {"o1": "SHIPPED"}
    assert statuses(latest_change_per_order(changes(spark, rows[::-1]))) == {"o1": "SHIPPED"}
