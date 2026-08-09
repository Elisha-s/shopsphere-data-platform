from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.getOrCreate()

    bronze = spark.table(
        "adb_shopsphere_dev.bronze.eventhub_order_events"
    )
    silver = spark.table(
        "adb_shopsphere_dev.silver.orders"
    )
    invalid = spark.table(
        "adb_shopsphere_dev.silver.invalid_orders"
    )
    current_orders = spark.table(
        "adb_shopsphere_dev.silver.current_orders"
    )

    bronze_count = bronze.count()
    silver_count = silver.count()
    invalid_count = invalid.count()
    current_count = current_orders.count()

    bronze_unique_count = (
    bronze
    .select("event_id")
    .distinct()
    .count()
)

    duplicate_count = bronze_count - bronze_unique_count

    if bronze_unique_count != silver_count + invalid_count:
        raise RuntimeError(
            f"Reconciliation failed: "
            f"bronze={bronze_count}, "
            f"unique_bronze={bronze_unique_count}, "
            f"duplicates_removed={duplicate_count}, "
            f"silver={silver_count}, "
            f"invalid={invalid_count}"
            )
    print(
        f"""
            ShopSphere reconciliation passed:
            Bronze records      : {bronze_count}
            Unique events       : {bronze_unique_count}
            Duplicates removed  : {duplicate_count}
            Silver valid        : {silver_count}
            Silver invalid      : {invalid_count}
        """
    )

    if current_count == 0:
        raise RuntimeError("current_orders is empty")

    gold_tables = [
        "adb_shopsphere_dev.gold.order_activity_by_status",
        "adb_shopsphere_dev.gold.customer_metrics",
        "adb_shopsphere_dev.gold.product_metrics",
    ]

    for table_name in gold_tables:
        if spark.table(table_name).count() == 0:
            raise RuntimeError(
                f"Validation failed: {table_name} is empty"
            )

    print("ShopSphere validation passed.")


if __name__ == "__main__":
    main()