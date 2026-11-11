"""End-to-end test of the local pipeline on real Delta tables.

Run with:  pytest -m delta

It needs the Delta Lake libraries, which Spark downloads from Maven Central
the first time (about a minute). It covers what the fast tests cannot: the
Change Data Feed read, the MERGE into current_orders, and the saved
processing position.
"""

import pytest

from local_pipeline import stages
from local_pipeline.config import LocalConfig
from local_pipeline.run_pipeline import run_once
from local_pipeline.spark_session import create_spark_session
from shopsphere_data_platform_etl.core.tables import table_names
from shopsphere_data_platform_etl.jobs import cdf_merge
from tests.helpers import generate_landing_files

pytestmark = pytest.mark.delta


@pytest.fixture(scope="module")
def pipeline(tmp_path_factory):
    config = LocalConfig(data_dir=str(tmp_path_factory.mktemp("delta")), source="files")
    spark = create_spark_session(config)
    if "DeltaSparkSessionExtension" not in spark.conf.get("spark.sql.extensions", ""):
        pytest.skip("A non-Delta Spark session is already running in this process; run `pytest -m delta` on its own.")
    yield spark, config, table_names()
    spark.stop()


def test_full_pipeline_twice(pipeline):
    spark, config, tables = pipeline

    # ---- First run -----------------------------------------------------
    first = generate_landing_files(config.landing_dir, ticks=1500, seed=21)
    counts = run_once(spark, config)  # raises if validation fails

    delivered = first["events_delivered"]
    assert counts["bronze.order_events"] == delivered
    assert counts["silver.invalid_orders"] == first["invalid"]
    assert counts["silver.orders"] == delivered - first["duplicates"] - first["invalid"]
    assert 0 < counts["silver.current_orders"] <= first["orders_created"]

    assert cdf_merge.read_last_processed_version(spark, tables) is not None

    # ---- Running again with no new data changes nothing ---------------------
    position = cdf_merge.read_last_processed_version(spark, tables)
    rerun = run_once(spark, config)
    # (The per-minute activity table is left out: a watermarked aggregation may
    # legitimately release a few more closed windows on a later run.)
    stable = [name for name in counts if name != "gold.order_activity_by_status"]
    assert {name: rerun[name] for name in stable} == {name: counts[name] for name in stable}
    assert cdf_merge.run(spark, tables)["status"] == "up_to_date"
    assert cdf_merge.read_last_processed_version(spark, tables) == position

    # ---- Second batch: new orders arrive, existing counts only grow ----------
    second = generate_landing_files(config.landing_dir, ticks=800, seed=22)
    counts_after = run_once(spark, config)
    delivered += second["events_delivered"]
    assert counts_after["bronze.order_events"] == delivered
    assert counts_after["silver.current_orders"] > counts["silver.current_orders"]
    assert cdf_merge.read_last_processed_version(spark, tables) > position

    # ---- One row per order, and it is the newest state -------------------
    current = spark.table(tables.silver_current_orders)
    assert current.count() == current.select("order_id").distinct().count()
    assert all(check["passed"] for check in stages.run_validation(spark, tables))


def test_replaying_processed_versions_is_harmless(pipeline):
    """Simulates a crash after the MERGE but before the position was saved."""
    spark, config, tables = pipeline
    before = {row["order_id"]: row["event_id"] for row in spark.table(tables.silver_current_orders).collect()}

    spark.sql(f"DELETE FROM {tables.pipeline_state} WHERE pipeline_name = '{cdf_merge.PIPELINE_NAME}'")
    assert cdf_merge.run(spark, tables)["status"] == "merged"  # reprocesses from version 0

    after = {row["order_id"]: row["event_id"] for row in spark.table(tables.silver_current_orders).collect()}
    assert after == before


def test_a_recreated_source_table_fails_loudly(pipeline):
    spark, config, tables = pipeline
    cdf_merge.save_last_processed_version(spark, tables, 10_000)
    with pytest.raises(RuntimeError, match="probably recreated"):
        cdf_merge.run(spark, tables)
